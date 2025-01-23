use std::{
	borrow::Cow,
	collections::HashMap,
	fs::{File, OpenOptions},
	io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
	num::{NonZeroU16, NonZeroU64},
	path::Path,
};

use static_assertions::assert_impl_all;
use zerocopy::{FromBytes, Immutable, IntoBytes};

const FORMAT_VERSION: u8 = 1;

#[cfg(test)]
use mockall::automock;

use crate::{
	repr::{IoRepr, Repr},
	utils::units::MIB,
};

use super::{
	generic::{FileType, GenericHeader, GenericHeaderRepr},
	utils::CRC32,
	FileError, PageAddress, TransactionState, WalIndex,
};

const FLAG_UNDO: u8 = 0b00000001;

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct ItemHeaderRepr {
	kind: u8,
	flags: u8,
	body_length: u16,
	crc: u32,
	prev_item: Option<NonZeroU64>,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct ItemFooterRepr {
	item_start: u64,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct TransactionBlockRepr {
	transaction_id: u64,
	prev_transaction_generation: u64,
	prev_transaction_offset: Option<NonZeroU64>,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C, packed)]
struct WriteBlockRepr {
	segment_num: u32,
	page_num: u16,
	offset: u16,
	write_length: u16,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct CheckpointBlockRepr {
	num_dirty_pages: u64,
	num_transactions: u64,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C, packed)]
struct PageAddressRepr {
	segment_num: u32,
	page_num: u16,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct WalIndexRepr {
	generation: u64,
	offset: u64,
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C)]
struct TransactionStateRepr {
	first_generation: u64,
	last_generation: u64,
	last_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ItemKind {
	Write = 0,
	Commit = 1,
	Checkpoint = 2,
}

impl TryFrom<u8> for ItemKind {
	type Error = FileError;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Write),
			1 => Ok(Self::Commit),
			2 => Ok(Self::Checkpoint),
			_ => Err(FileError::Corrupted(format!(
				"Unknown WAL item kind {value}"
			))),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemHeader {
	kind: ItemKind,
	flags: u8,
	body_length: u16,
	crc: u32,
	prev_item: Option<NonZeroU64>,
}

impl From<ItemHeader> for ItemHeaderRepr {
	fn from(value: ItemHeader) -> Self {
		Self {
			kind: value.kind as u8,
			flags: value.flags,
			body_length: value.body_length,
			crc: value.crc,
			prev_item: value.prev_item,
		}
	}
}

impl TryFrom<ItemHeaderRepr> for ItemHeader {
	type Error = FileError;

	fn try_from(value: ItemHeaderRepr) -> Result<Self, Self::Error> {
		Ok(Self {
			kind: ItemKind::try_from(value.kind)?,
			flags: value.flags,
			body_length: value.body_length,
			crc: value.crc,
			prev_item: value.prev_item,
		})
	}
}

impl Repr<ItemHeader> for ItemHeaderRepr {
	type Error = FileError;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemFooter {
	item_start: NonZeroU64,
}

impl From<ItemFooter> for ItemFooterRepr {
	fn from(value: ItemFooter) -> Self {
		Self {
			item_start: value.item_start.get(),
		}
	}
}

impl TryFrom<ItemFooterRepr> for ItemFooter {
	type Error = FileError;

	fn try_from(value: ItemFooterRepr) -> Result<Self, Self::Error> {
		let Some(item_start) = NonZeroU64::new(value.item_start) else {
			return Err(FileError::Corrupted(
				"WAL items cannot start at position 0".to_string(),
			));
		};
		Ok(Self { item_start })
	}
}

impl Repr<ItemFooter> for ItemFooterRepr {
	type Error = FileError;
}

struct TransactionBlock {
	transaction_id: u64,
	prev_transaction_item: Option<WalIndex>,
}

impl From<TransactionBlock> for TransactionBlockRepr {
	fn from(value: TransactionBlock) -> Self {
		Self {
			transaction_id: value.transaction_id,
			prev_transaction_generation: value
				.prev_transaction_item
				.map(|idx| idx.generation)
				.unwrap_or_default(),
			prev_transaction_offset: value.prev_transaction_item.map(|idx| idx.offset),
		}
	}
}

impl From<TransactionBlockRepr> for TransactionBlock {
	fn from(value: TransactionBlockRepr) -> Self {
		Self {
			transaction_id: value.transaction_id,
			prev_transaction_item: value
				.prev_transaction_offset
				.map(|offset| WalIndex::new(value.prev_transaction_generation, offset)),
		}
	}
}

impl Repr<TransactionBlock> for TransactionBlockRepr {
	type Error = FileError;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteBlock {
	page_address: PageAddress,
	offset: u16,
	write_length: u16,
}

impl From<WriteBlock> for WriteBlockRepr {
	fn from(value: WriteBlock) -> Self {
		Self {
			segment_num: value.page_address.segment_num,
			page_num: value.page_address.page_num.get(),
			offset: value.offset,
			write_length: value.write_length,
		}
	}
}

impl TryFrom<WriteBlockRepr> for WriteBlock {
	type Error = FileError;

	fn try_from(value: WriteBlockRepr) -> Result<Self, Self::Error> {
		let Some(page_num) = NonZeroU16::new(value.page_num) else {
			return Err(FileError::Corrupted(
				"0 is not a valid page number".to_string(),
			));
		};
		Ok(Self {
			page_address: PageAddress::new(value.segment_num, page_num),
			offset: value.offset,
			write_length: value.write_length,
		})
	}
}

impl Repr<WriteBlock> for WriteBlockRepr {
	type Error = FileError;
}

type CheckpointBlock = CheckpointBlockRepr;

impl Repr<CheckpointBlock> for CheckpointBlockRepr {
	type Error = FileError;
}

impl From<PageAddress> for PageAddressRepr {
	fn from(value: PageAddress) -> Self {
		Self {
			segment_num: value.segment_num,
			page_num: value.page_num.get(),
		}
	}
}

impl TryFrom<PageAddressRepr> for PageAddress {
	type Error = FileError;

	fn try_from(value: PageAddressRepr) -> Result<Self, Self::Error> {
		let Some(page_num) = NonZeroU16::new(value.page_num) else {
			return Err(FileError::Corrupted(
				"Found invalid page number 0".to_string(),
			));
		};
		Ok(PageAddress::new(value.segment_num, page_num))
	}
}

impl Repr<PageAddress> for PageAddressRepr {
	type Error = FileError;
}

impl From<WalIndex> for WalIndexRepr {
	fn from(value: WalIndex) -> Self {
		Self {
			offset: value.offset.get(),
			generation: value.generation,
		}
	}
}

impl TryFrom<WalIndexRepr> for WalIndex {
	type Error = FileError;

	fn try_from(value: WalIndexRepr) -> Result<Self, Self::Error> {
		let Some(offset) = NonZeroU64::new(value.offset) else {
			return Err(FileError::Corrupted(
				"Found invalid WAL offset '0'".to_string(),
			));
		};
		Ok(Self {
			generation: value.generation,
			offset,
		})
	}
}

impl Repr<WalIndex> for WalIndexRepr {
	type Error = FileError;
}

impl From<TransactionState> for TransactionStateRepr {
	fn from(value: TransactionState) -> Self {
		Self {
			first_generation: value.first_gen,
			last_generation: value.last_index.generation,
			last_offset: value.last_index.offset.get(),
		}
	}
}

impl Repr<TransactionState> for TransactionStateRepr {
	type Error = FileError;
}

impl TryFrom<TransactionStateRepr> for TransactionState {
	type Error = FileError;

	fn try_from(value: TransactionStateRepr) -> Result<Self, Self::Error> {
		let Some(last_offset) = NonZeroU64::new(value.last_offset) else {
			return Err(FileError::Corrupted(
				"Found invalid WAL offset '0'".to_string(),
			));
		};
		Ok(Self {
			first_gen: value.first_generation,
			last_index: WalIndex::new(value.last_generation, last_offset),
		})
	}
}

const WRITE_BUF_LIMIT: usize = 2 * MIB;

pub(crate) struct WalFile<F: Seek + Read + Write = File> {
	body_start: u64,
	prev_item: Option<NonZeroU64>,
	write_buf: Vec<u8>,
	file: F,
	next_offset: NonZeroU64,
}
assert_impl_all!(WalFile: Send, Sync);

impl WalFile {
	pub fn create_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		Self::create(
			OpenOptions::new()
				.create(true)
				.truncate(true)
				.read(true)
				.write(true)
				.open(path)?,
		)
	}

	pub fn open_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		Self::open(OpenOptions::new().read(true).write(true).open(path)?)
	}
}

impl<F: Seek + Read + Write> WalFile<F> {
	fn create(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let content_offset = u16::try_from(GenericHeaderRepr::SIZE).unwrap();
		let meta = GenericHeader {
			file_type: FileType::Wal,
			content_offset,
			version: FORMAT_VERSION,
		};
		GenericHeaderRepr::serialize(meta, &mut file)?;
		Self::new(file, content_offset.into())
	}

	fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let header = GenericHeaderRepr::deserialize(&mut file)?;
		if header.file_type != FileType::Wal {
			return Err(FileError::WrongFileType(header.file_type));
		}
		if header.version != FORMAT_VERSION {
			return Err(FileError::IncompatibleVersion(
				header.file_type,
				header.version,
			));
		}

		Self::new(file, header.content_offset.into())
	}

	fn new(mut file: F, body_start: u64) -> Result<Self, FileError> {
		let prev_footer_start =
			file.seek(SeekFrom::End(-i64::try_from(ItemFooterRepr::SIZE).unwrap()))?;
		let prev_item = if prev_footer_start > body_start {
			let footer = ItemFooterRepr::deserialize(&mut file)?;
			Some(footer.item_start)
		} else {
			None
		};
		let next_offset = NonZeroU64::new(file.seek(SeekFrom::End(0))?).unwrap();
		Ok(Self {
			body_start,
			file,
			write_buf: Vec::new(),
			prev_item,
			next_offset,
		})
	}

	fn write_transaction_block(writer: impl Write, data: TransactionData) -> Result<(), FileError> {
		let block = TransactionBlock {
			transaction_id: data.transaction_id,
			prev_transaction_item: data.prev_transaction_item,
		};
		TransactionBlockRepr::serialize(block, writer)?;
		Ok(())
	}

	fn write_write_block(mut writer: impl Write, data: WriteData) -> Result<(), FileError> {
		Self::write_transaction_block(&mut writer, data.transaction_data)?;

		let block = WriteBlock {
			page_address: data.page_address,
			offset: data.offset,
			write_length: data
				.to
				.len()
				.try_into()
				.expect("Write length must be 16-bit!"),
		};
		WriteBlockRepr::serialize(block, &mut writer)?;
		if let Some(from) = data.from {
			debug_assert_eq!(from.len(), data.to.len());
			writer.write_all(&from)?;
		}
		writer.write_all(&data.to)?;
		Ok(())
	}

	fn write_checkpoint_block(
		mut writer: impl Write,
		data: CheckpointData,
	) -> Result<(), FileError> {
		let block = CheckpointBlock {
			num_dirty_pages: data.dirty_pages.len() as u64,
			num_transactions: data.transactions.len() as u64,
		};
		CheckpointBlockRepr::serialize(block, &mut writer)?;
		for (page_address, wal_index) in data.dirty_pages.iter() {
			PageAddressRepr::serialize(*page_address, &mut writer)?;
			WalIndexRepr::serialize(*wal_index, &mut writer)?;
		}
		for (transaction_id, transaction_state) in data.transactions.iter() {
			writer.write_all(transaction_id.as_bytes())?;
			TransactionStateRepr::serialize(transaction_state.clone(), &mut writer)?;
		}

		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionData {
	pub transaction_id: u64,
	pub prev_transaction_item: Option<WalIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteData<'a> {
	pub transaction_data: TransactionData,
	pub page_address: PageAddress,
	pub offset: u16,
	pub from: Option<Cow<'a, [u8]>>,
	pub to: Cow<'a, [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointData<'a> {
	pub transactions: Cow<'a, HashMap<u64, TransactionState>>,
	pub dirty_pages: Cow<'a, HashMap<PageAddress, WalIndex>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item<'a> {
	Write(WriteData<'a>),
	Commit(TransactionData),
	Checkpoint(CheckpointData<'a>),
}

#[cfg_attr(test, automock(
    type IterItems<'a> = std::vec::IntoIter<Result<(NonZeroU64, Item<'static>), FileError>>;
    type IterItemsReverse<'a> = std::iter::Rev<std::vec::IntoIter<Result<(NonZeroU64, Item<'static>), FileError>>>;
), allow(clippy::type_complexity))]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	type IterItems<'a>: Iterator<Item = Result<(NonZeroU64, Item<'static>), FileError>> + 'a
	where
		Self: 'a;
	type IterItemsReverse<'a>: Iterator<Item = Result<(NonZeroU64, Item<'static>), FileError>> + 'a
	where
		Self: 'a;

	fn push_item<'a>(&mut self, item: Item<'a>) -> Result<NonZeroU64, FileError>;
	fn flush(&mut self) -> Result<(), FileError>;
	fn read_item_at(&mut self, offset: NonZeroU64) -> Result<Item<'static>, FileError>;
	fn iter_items<'a>(&'a mut self) -> Result<Self::IterItems<'a>, FileError>;
	fn iter_items_reverse<'a>(&'a mut self) -> Result<Self::IterItemsReverse<'a>, FileError>;
	fn next_offset(&self) -> NonZeroU64;
	fn size(&self) -> usize;
}

impl<F: Seek + Read + Write> WalFileApi for WalFile<F> {
	type IterItems<'a> = IterItems<&'a mut F> where F: 'a;
	type IterItemsReverse<'a> = IterItemsReverse<&'a mut F> where F: 'a;

	fn push_item(&mut self, item: Item<'_>) -> Result<NonZeroU64, FileError> {
		let current_pos = self.next_offset;

		let mut body_buffer: Vec<u8> = vec![];
		let kind: ItemKind;
		let mut flags: u8 = 0;
		match item {
			Item::Write(write_data) => {
				kind = ItemKind::Write;
				if write_data.from.is_none() {
					flags |= FLAG_UNDO;
				}
				Self::write_write_block(&mut body_buffer, write_data)?;
			}
			Item::Commit(transaction_data) => {
				kind = ItemKind::Commit;
				Self::write_transaction_block(&mut body_buffer, transaction_data)?
			}
			Item::Checkpoint(checkpoint_data) => {
				kind = ItemKind::Checkpoint;
				Self::write_checkpoint_block(&mut body_buffer, checkpoint_data)?
			}
		};
		let crc = CRC32.checksum(&body_buffer);

		let item_header = ItemHeader {
			kind,
			flags,
			body_length: body_buffer
				.len()
				.try_into()
				.expect("WAL item body length must be 16-bit!"),
			crc,
			prev_item: self.prev_item,
		};
		ItemHeaderRepr::serialize(item_header, &mut self.write_buf)?;

		self.write_buf.write_all(&body_buffer)?;

		let item_footer = ItemFooter {
			item_start: current_pos,
		};
		ItemFooterRepr::serialize(item_footer, &mut self.write_buf)?;

		self.prev_item = Some(current_pos);

		self.next_offset =
			NonZeroU64::new(self.file.seek(SeekFrom::End(0))? + self.write_buf.len() as u64)
				.expect("WAL file unexpectedly at position 0");

		if self.write_buf.len() < WRITE_BUF_LIMIT {
			self.flush()?;
		}

		Ok(current_pos)
	}

	fn flush(&mut self) -> Result<(), FileError> {
		self.file.write_all(&self.write_buf)?;
		self.write_buf.clear();
		Ok(())
	}

	fn read_item_at(&mut self, offset: NonZeroU64) -> Result<Item<'static>, FileError> {
		debug_assert!(offset.get() >= self.body_start);

		self.flush()?;
		self.file.seek(SeekFrom::Start(offset.get()))?;
		let mut reader = ItemReader::new(&mut self.file, None)?;
		let Some((read_offset, item)) = reader.read_item()? else {
			return Err(FileError::UnexpectedEof);
		};
		debug_assert_eq!(read_offset, offset);

		Ok(item)
	}

	fn iter_items(&mut self) -> Result<Self::IterItems<'_>, FileError> {
		self.flush()?;
		self.file.seek(SeekFrom::Start(self.body_start))?;
		IterItems::new(&mut self.file)
	}

	fn iter_items_reverse(&mut self) -> Result<Self::IterItemsReverse<'_>, FileError> {
		self.flush()?;
		self.file.seek(SeekFrom::End(0))?;
		IterItemsReverse::new(&mut self.file, self.prev_item)
	}

	#[inline]
	fn size(&self) -> usize {
		usize::try_from(self.next_offset.get()).expect("Wal size exceeded usize::MAX")
	}

	#[inline]
	fn next_offset(&self) -> NonZeroU64 {
		self.next_offset
	}
}

struct ItemReader<F: Read + Seek> {
	offset: u64,
	reader: BufReader<F>,
	prev_item: Option<NonZeroU64>,
}

impl<F: Read + Seek> ItemReader<F> {
	fn new(mut file: F, prev_item: Option<NonZeroU64>) -> Result<Self, FileError> {
		let offset = file.stream_position()?;
		Ok(Self {
			offset,
			reader: BufReader::new(file),
			prev_item,
		})
	}

	fn read_transaction_data(body: impl Read) -> Result<TransactionData, FileError> {
		let transaction_block = TransactionBlockRepr::deserialize(body)?;

		Ok(TransactionData {
			transaction_id: transaction_block.transaction_id,
			prev_transaction_item: transaction_block.prev_transaction_item,
		})
	}

	fn read_write_data(
		mut body: impl Read,
		is_undo: bool,
	) -> Result<WriteData<'static>, FileError> {
		let transaction_data = Self::read_transaction_data(&mut body)?;

		let write_block = WriteBlockRepr::deserialize(&mut body)?;
		let from: Option<Vec<u8>> = if is_undo {
			None
		} else {
			let mut from = vec![0; write_block.write_length.into()];
			body.read_exact(&mut from)?;
			Some(from)
		};
		let mut to: Vec<u8> = vec![0; write_block.write_length.into()];
		body.read_exact(&mut to)?;

		Ok(WriteData {
			transaction_data,
			page_address: write_block.page_address,
			offset: write_block.offset,
			from: from.map(Cow::Owned),
			to: Cow::Owned(to),
		})
	}

	fn read_checkpoint_data(mut body: impl Read) -> Result<CheckpointData<'static>, FileError> {
		let checkpoint_block = CheckpointBlock::deserialize(&mut body)?;

		let mut dirty_pages: HashMap<PageAddress, WalIndex> = HashMap::new();
		for _ in 0..checkpoint_block.num_dirty_pages {
			let page_address = PageAddressRepr::deserialize(&mut body)?;
			let wal_index = WalIndexRepr::deserialize(&mut body)?;
			dirty_pages.insert(page_address, wal_index);
		}

		let mut transactions: HashMap<u64, TransactionState> = HashMap::new();
		for _ in 0..checkpoint_block.num_transactions {
			let mut tid_bytes = [0; 8];
			body.read_exact(&mut tid_bytes)?;
			let transaction_id = u64::from_ne_bytes(tid_bytes);
			let transaction_state = TransactionStateRepr::deserialize(&mut body)?;
			transactions.insert(transaction_id, transaction_state);
		}

		Ok(CheckpointData {
			dirty_pages: Cow::Owned(dirty_pages),
			transactions: Cow::Owned(transactions),
		})
	}

	fn read_item_exact(&mut self) -> Result<(NonZeroU64, Item<'static>), FileError> {
		let header = ItemHeaderRepr::deserialize(&mut self.reader)?;
		let mut body_buf: Box<[u8]> = vec![0; header.body_length.into()].into();
		self.reader.read_exact(&mut body_buf)?;
		self.prev_item = header.prev_item;

		if CRC32.checksum(&body_buf) != header.crc {
			return Err(FileError::ChecksumMismatch);
		}

		let is_undo = header.flags & FLAG_UNDO != 0;

		let mut body_cursor = Cursor::new(body_buf);
		let item = match header.kind {
			ItemKind::Write => Item::Write(Self::read_write_data(&mut body_cursor, is_undo)?),
			ItemKind::Commit => Item::Commit(Self::read_transaction_data(&mut body_cursor)?),
			ItemKind::Checkpoint => Item::Checkpoint(Self::read_checkpoint_data(&mut body_cursor)?),
		};

		self.reader
			.seek_relative(i64::try_from(ItemFooterRepr::SIZE).unwrap())?;

		let item_offset = self.offset;
		self.offset +=
			(ItemHeaderRepr::SIZE + header.body_length as usize + ItemFooterRepr::SIZE) as u64;

		Ok((
			NonZeroU64::new(item_offset).expect("WAL was unexpectedly read at offset 0"),
			item,
		))
	}

	fn read_item(&mut self) -> Result<Option<(NonZeroU64, Item<'static>)>, FileError> {
		match self.read_item_exact() {
			Err(FileError::UnexpectedEof) => Ok(None),
			Err(other_error) => Err(other_error),
			Ok(value) => Ok(Some(value)),
		}
	}

	fn read_prev_item(&mut self) -> Result<Option<(NonZeroU64, Item<'static>)>, FileError> {
		let Some(prev_item) = self.prev_item else {
			return Ok(None);
		};
		let relative_offset_abs = i64::try_from(prev_item.get().abs_diff(self.offset))
			.expect("WAL item size exceeded i64::MAX!");
		self.reader
			.seek_relative(if self.offset >= prev_item.get() {
				-relative_offset_abs
			} else {
				relative_offset_abs
			})?;
		self.offset = prev_item.get();
		self.read_item()
	}
}

pub(crate) struct IterItems<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> IterItems<F> {
	fn new(file: F) -> Result<Self, FileError> {
		Ok(Self {
			reader: ItemReader::new(file, None)?,
		})
	}
}

impl<F: Read + Seek> Iterator for IterItems<F> {
	type Item = Result<(NonZeroU64, Item<'static>), FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_item().transpose()
	}
}

pub(crate) struct IterItemsReverse<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> IterItemsReverse<F> {
	fn new(file: F, prev_item: Option<NonZeroU64>) -> Result<Self, FileError> {
		Ok(Self {
			reader: ItemReader::new(file, prev_item)?,
		})
	}
}

impl<F: Read + Seek> Iterator for IterItemsReverse<F> {
	type Item = Result<(NonZeroU64, Item<'static>), FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_prev_item().transpose()
	}
}

#[cfg(test)]
mod tests {
	use pretty_assertions::assert_buf_eq;

	use crate::{
		files::{
			generic::GenericHeaderRepr,
			test_helpers::{page_address, wal_index},
		},
		utils::test_helpers::non_zero,
	};

	use super::*;

	#[test]
	fn create_wal() {
		// given
		let mut file = Vec::<u8>::new();

		// when
		WalFile::create(Cursor::new(&mut file)).unwrap();

		// then
		let mut expected_data = Vec::<u8>::new();
		expected_data.extend(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Wal,
				content_offset: GenericHeaderRepr::SIZE as u16,
				version: FORMAT_VERSION,
			})
			.as_bytes(),
		);

		assert_eq!(file.len(), GenericHeaderRepr::SIZE);
		assert_buf_eq!(file, expected_data);
	}

	#[test]
	fn open_wal() {
		// given
		let mut file = Vec::<u8>::new();
		file.extend(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Wal,
				content_offset: GenericHeaderRepr::SIZE as u16,
				version: FORMAT_VERSION,
			})
			.as_bytes(),
		);

		// when
		let result = WalFile::open(Cursor::new(&mut file));

		// then
		assert!(result.is_ok());
	}

	#[test]
	fn push_write_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		wal_file
			.push_item(Item::Write(WriteData {
				transaction_data: TransactionData {
					transaction_id: 25,
					prev_transaction_item: Some(wal_index!(123, 24)),
				},
				page_address: page_address!(123, 456),
				offset: 445,
				from: Some(Cow::Owned(vec![1, 2, 3, 4])),
				to: Cow::Owned(vec![4, 5, 6, 7]),
			}))
			.unwrap();
		wal_file.flush().unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Write as u8,
				flags: 0,
				body_length: 42,
				crc: 0x994f0abc,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_generation: 123,
				prev_transaction_offset: NonZeroU64::new(24),
				transaction_id: 25,
			}
			.as_bytes(),
		);
		expected_body.extend(
			WriteBlockRepr {
				segment_num: 123,
				page_num: 456,
				offset: 445,
				write_length: 4,
			}
			.as_bytes(),
		);
		expected_body.extend([1, 2, 3, 4]);
		expected_body.extend([4, 5, 6, 7]);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeaderRepr::SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_buf_eq!(&file[GenericHeaderRepr::SIZE..], expected_body);
	}

	#[test]
	fn push_commit_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		wal_file
			.push_item(Item::Commit(TransactionData {
				transaction_id: 69,
				prev_transaction_item: Some(wal_index!(123, 25)),
			}))
			.unwrap();
		wal_file.flush().unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Commit as u8,
				flags: 0,
				body_length: 24,
				crc: 0x8b777949,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_generation: 123,
				prev_transaction_offset: NonZeroU64::new(25),
				transaction_id: 69,
			}
			.as_bytes(),
		);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeaderRepr::SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_buf_eq!(&file[GenericHeaderRepr::SIZE..], expected_body);
	}

	#[test]
	fn push_undo_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		wal_file
			.push_item(Item::Write(WriteData {
				transaction_data: TransactionData {
					transaction_id: 25,
					prev_transaction_item: Some(wal_index!(123, 24)),
				},
				page_address: page_address!(123, 456),
				offset: 445,
				from: None,
				to: vec![4, 5, 6, 7].into(),
			}))
			.unwrap();
		wal_file.flush().unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Write as u8,
				flags: FLAG_UNDO,
				body_length: 38,
				crc: 0x1af2b54e,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_generation: 123,
				prev_transaction_offset: NonZeroU64::new(24),
				transaction_id: 25,
			}
			.as_bytes(),
		);
		expected_body.extend(
			WriteBlockRepr {
				offset: 445,
				segment_num: 123,
				page_num: 456,
				write_length: 4,
			}
			.as_bytes(),
		);
		expected_body.extend([4, 5, 6, 7]);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeaderRepr::SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_buf_eq!(&file[GenericHeaderRepr::SIZE..], expected_body);
	}

	#[test]
	fn push_checkpoint_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		let mut dirty_pages = HashMap::new();
		dirty_pages.insert(page_address!(1, 2), wal_index!(0, 3));
		let mut transactions = HashMap::new();
		transactions.insert(
			69,
			TransactionState {
				first_gen: 0,
				last_index: wal_index!(1, 420),
			},
		);
		wal_file
			.push_item(Item::Checkpoint(CheckpointData {
				dirty_pages: Cow::Borrowed(&dirty_pages),
				transactions: Cow::Borrowed(&transactions),
			}))
			.unwrap();
		wal_file.flush().unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Checkpoint as u8,
				flags: 0,
				body_length: 70,
				crc: 0x3420af22,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			CheckpointBlockRepr {
				num_dirty_pages: 1,
				num_transactions: 1,
			}
			.as_bytes(),
		);
		expected_body.extend(
			PageAddressRepr {
				segment_num: 1,
				page_num: 2,
			}
			.as_bytes(),
		);
		expected_body.extend(
			WalIndexRepr {
				generation: 0,
				offset: 3,
			}
			.as_bytes(),
		);
		expected_body.extend(69_u64.to_ne_bytes());
		expected_body.extend(
			TransactionStateRepr {
				first_generation: 0,
				last_generation: 1,
				last_offset: 420,
			}
			.as_bytes(),
		);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeaderRepr::SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_buf_eq!(&file[GenericHeaderRepr::SIZE..], expected_body);
	}

	#[test]
	fn write_and_read() {
		// given
		let mut wal_file = WalFile::create(Cursor::new(Vec::new())).unwrap();
		let item = Item::Write(WriteData {
			transaction_data: TransactionData {
				transaction_id: 0,
				prev_transaction_item: None,
			},
			page_address: page_address!(123, 456),
			offset: 420,
			from: Some(Cow::Owned(vec![0, 0, 0, 0])),
			to: Cow::Owned(vec![1, 2, 3, 4]),
		});

		// when
		let offset = wal_file.push_item(item.clone()).unwrap();
		wal_file.flush().unwrap();

		// then
		assert_eq!(wal_file.read_item_at(offset).unwrap(), item)
	}

	#[test]
	fn write_and_iter() {
		// given
		let mut wal_file = WalFile::create(Cursor::new(Vec::new())).unwrap();
		let items = [
			Item::Write(WriteData {
				transaction_data: TransactionData {
					transaction_id: 0,
					prev_transaction_item: None,
				},
				page_address: page_address!(123, 456),
				offset: 420,
				from: Some(Cow::Owned(vec![0, 0, 0, 0])),
				to: Cow::Owned(vec![1, 2, 3, 4]),
			}),
			Item::Commit(TransactionData {
				transaction_id: 0,
				prev_transaction_item: None,
			}),
		];

		// when
		for item in &items {
			wal_file.push_item(item.clone()).unwrap();
		}
		wal_file.flush().unwrap();

		// then
		let mut iter = wal_file.iter_items().unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(non_zero!(9), items[0].clone())
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(non_zero!(75), items[1].clone())
		);
		assert!(dbg!(iter.next()).is_none());
	}

	#[test]
	fn write_and_iter_reverse() {
		// given
		let mut wal_file = WalFile::create(Cursor::new(Vec::new())).unwrap();
		let items = [
			Item::Write(WriteData {
				transaction_data: TransactionData {
					transaction_id: 0,
					prev_transaction_item: None,
				},
				page_address: page_address!(123, 456),
				offset: 420,
				from: Some(Cow::Owned(vec![0, 0, 0, 0])),
				to: Cow::Owned(vec![1, 2, 3, 4]),
			}),
			Item::Commit(TransactionData {
				transaction_id: 0,
				prev_transaction_item: None,
			}),
		];

		// when
		for item in &items {
			wal_file.push_item(item.clone()).unwrap();
		}
		wal_file.flush().unwrap();

		// then
		let mut iter = wal_file.iter_items_reverse().unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(non_zero!(75), items[1].clone())
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(non_zero!(9), items[0].clone())
		);
		assert!(iter.next().is_none());
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	macro_rules! mock_wal_file {
		($($offset:expr => $item:expr),* $(,)?) => {{
			let mut file = $crate::files::wal::MockWalFileApi::new();
            file.expect_iter_items().returning(|| {
                Ok(vec![
                   $(Ok(($crate::utils::test_helpers::non_zero!($offset), $item))),*
                ].into_iter())
            });
            file.expect_iter_items_reverse().returning(|| {
                Ok(vec![
                   $(Ok(($crate::utils::test_helpers::non_zero!($offset), $item))),*
                ].into_iter().rev())
            });
            $(
                file
                    .expect_read_item_at()
                    .with(eq($crate::utils::test_helpers::non_zero!($offset)))
                    .returning(|_| Ok($item));
            )*
            file
		}};
	}
	pub(crate) use mock_wal_file;
}
