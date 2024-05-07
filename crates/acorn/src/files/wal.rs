use std::{
	borrow::Cow,
	collections::HashMap,
	fs::{File, OpenOptions},
	io::{BufRead, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
	mem,
	num::{NonZeroU16, NonZeroU64},
	path::Path,
};

use static_assertions::assert_impl_all;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

const FORMAT_VERSION: u8 = 1;

#[cfg(test)]
use mockall::automock;

use crate::storage::{PageId, WalIndex};

use super::{
	generic::{FileType, GenericHeader},
	utils::{Serialized, CRC32},
	FileError,
};

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct ItemHeaderRepr {
	kind: u8,
	_padding: u8,
	body_length: u16,
	crc: u32,
	prev_item: Option<NonZeroU64>,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct ItemFooterRepr {
	item_start: u64,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct TransactionBlockRepr {
	transaction_id: u64,
	prev_transaction_item: Option<NonZeroU64>,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C, packed)]
struct WriteBlockRepr {
	page_id: u64,
	offset: u16,
	write_length: u16,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct CheckpointBlockRepr {
	num_dirty_pages: u64,
	num_transactions: u64,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C, packed)]
pub(crate) struct PageIdRepr {
	segment_num: u32,
	page_num: u16,
}

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
pub(crate) struct WalIndexRepr {
	generation: u64,
	offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ItemKind {
	Write = 0,
	Commit = 1,
	Undo = 2,
	Checkpoint = 3,
}

impl TryFrom<u8> for ItemKind {
	type Error = FileError;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Write),
			1 => Ok(Self::Commit),
			2 => Ok(Self::Undo),
			3 => Ok(Self::Checkpoint),
			_ => Err(FileError::Corrupted(format!(
				"Unknown WAL item kind {value}"
			))),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ItemHeader {
	kind: ItemKind,
	body_length: u16,
	crc: u32,
	prev_item: Option<NonZeroU64>,
}

impl From<ItemHeader> for ItemHeaderRepr {
	fn from(value: ItemHeader) -> Self {
		Self {
			kind: value.kind as u8,
			_padding: 0,
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
			body_length: value.body_length,
			crc: value.crc,
			prev_item: value.prev_item,
		})
	}
}

impl Serialized for ItemHeader {
	type Repr = ItemHeaderRepr;
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

impl Serialized for ItemFooter {
	type Repr = ItemFooterRepr;
}

type TransactionBlock = TransactionBlockRepr;

impl Serialized for TransactionBlock {
	type Repr = TransactionBlockRepr;
}

type WriteBlock = WriteBlockRepr;

impl Serialized for WriteBlock {
	type Repr = WriteBlockRepr;
}

type CheckpointBlock = CheckpointBlockRepr;

impl Serialized for CheckpointBlock {
	type Repr = CheckpointBlockRepr;
}

impl From<PageId> for PageIdRepr {
	fn from(value: PageId) -> Self {
		Self {
			segment_num: value.segment_num,
			page_num: value.page_num.get(),
		}
	}
}

impl TryFrom<PageIdRepr> for PageId {
	type Error = FileError;

	fn try_from(value: PageIdRepr) -> Result<Self, Self::Error> {
		let Some(page_num) = NonZeroU16::new(value.page_num) else {
			return Err(FileError::Corrupted(
				"Found invalid page number 0".to_string(),
			));
		};
		Ok(PageId::new(value.segment_num, page_num))
	}
}

impl Serialized for PageId {
	type Repr = PageIdRepr;
}

impl From<WalIndex> for WalIndexRepr {
	fn from(value: WalIndex) -> Self {
		Self {
			offset: value.offset,
			generation: value.generation,
		}
	}
}

impl From<WalIndexRepr> for WalIndex {
	fn from(value: WalIndexRepr) -> Self {
		Self {
			offset: value.offset,
			generation: value.generation,
		}
	}
}

impl Serialized for WalIndex {
	type Repr = WalIndexRepr;
}

pub(crate) struct WalFile<F: Seek + Read + Write = File> {
	body_start: u64,
	prev_item: Option<NonZeroU64>,
	file: F,
	size: usize,
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
		let content_offset = GenericHeader::REPR_SIZE as u16;
		let meta = GenericHeader {
			file_type: FileType::Wal,
			content_offset,
			version: FORMAT_VERSION,
		};
		meta.serialize(&mut file)?;
		Self::new(file, content_offset.into())
	}

	fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let header = GenericHeader::deserialize(&mut file)?;
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
		let prev_footer_start = file.seek(SeekFrom::End(-(ItemFooter::REPR_SIZE as i64)))?;
		let prev_item = if prev_footer_start > body_start {
			let footer = ItemFooter::deserialize(&mut file)?;
			Some(footer.item_start)
		} else {
			None
		};
		let size = file.seek(SeekFrom::End(0))? as usize;
		Ok(Self {
			body_start,
			file,
			prev_item,
			size,
		})
	}

	fn write_transaction_block(writer: impl Write, data: TransactionData) -> Result<(), FileError> {
		let block = TransactionBlock {
			transaction_id: data.transaction_id,
			prev_transaction_item: data.prev_transaction_item,
		};
		block.serialize(writer)?;
		Ok(())
	}

	fn write_write_block(mut writer: impl Write, data: WriteData) -> Result<(), FileError> {
		assert_eq!(data.from.len(), data.to.len());

		Self::write_transaction_block(&mut writer, data.transaction_data)?;

		let block = WriteBlock {
			page_id: data.page_id,
			offset: data.offset,
			write_length: data
				.from
				.len()
				.try_into()
				.expect("Write length must be 16-bit!"),
		};
		block.serialize(&mut writer)?;
		writer.write_all(&data.from)?;
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
		block.serialize(&mut writer)?;
		for (page_id, wal_index) in data.dirty_pages.iter() {
			page_id.serialize(&mut writer)?;
			wal_index.serialize(&mut writer)?;
		}
		for (transaction_id, wal_index) in data.transactions.iter() {
			writer.write_all(transaction_id.as_bytes())?;
			wal_index.serialize(&mut writer)?;
		}

		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionData {
	pub transaction_id: u64,
	pub prev_transaction_item: Option<NonZeroU64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteData<'a> {
	pub transaction_data: TransactionData,
	pub page_id: u64,
	pub offset: u16,
	pub from: Cow<'a, [u8]>,
	pub to: Cow<'a, [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointData<'a> {
	pub transactions: Cow<'a, HashMap<u64, WalIndex>>,
	pub dirty_pages: Cow<'a, HashMap<PageId, WalIndex>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item<'a> {
	Write(WriteData<'a>),
	Commit(TransactionData),
	Undo(TransactionData),
	Checkpoint(CheckpointData<'a>),
}

#[cfg_attr(test, automock(
    type IterItems<'a> = std::vec::IntoIter<Result<Item<'static>, FileError>>;
    type IterItemsReverse<'a> = std::vec::IntoIter<Result<Item<'static>, FileError>>;
))]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	type IterItems<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;
	type IterItemsReverse<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;

	fn push_item<'a>(&mut self, item: Item<'a>) -> Result<NonZeroU64, FileError>;
	fn read_item_at(&mut self, offset: NonZeroU64) -> Result<Item<'static>, FileError>;
	fn iter_items<'a>(&'a mut self) -> Result<Self::IterItems<'a>, FileError>;
	fn iter_items_reverse<'a>(&'a mut self) -> Result<Self::IterItemsReverse<'a>, FileError>;
	fn size(&self) -> usize;
}

impl<F: Seek + Read + Write> WalFileApi for WalFile<F> {
	type IterItems<'a> = IterItems<&'a mut F> where F: 'a;
	type IterItemsReverse<'a> = IterItemsReverse<&'a mut F> where F: 'a;

	fn push_item(&mut self, item: Item<'_>) -> Result<NonZeroU64, FileError> {
		let current_pos = NonZeroU64::new(self.file.seek(SeekFrom::End(0))?)
			.expect("Cannot write at position 0!");
		let mut writer = BufWriter::new(&mut self.file);

		let mut body_buffer: Vec<u8> = vec![];
		let kind: ItemKind;
		match item {
			Item::Write(write_data) => {
				kind = ItemKind::Write;
				Self::write_write_block(&mut body_buffer, write_data)?;
			}
			Item::Commit(transaction_data) => {
				kind = ItemKind::Commit;
				Self::write_transaction_block(&mut body_buffer, transaction_data)?
			}
			Item::Undo(transaction_data) => {
				kind = ItemKind::Undo;
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
			body_length: body_buffer
				.len()
				.try_into()
				.expect("WAL item body length must be 16-bit!"),
			crc,
			prev_item: self.prev_item,
		};
		item_header.serialize(&mut writer)?;

		writer.write_all(&body_buffer)?;

		let item_footer = ItemFooter {
			item_start: current_pos,
		};
		item_footer.serialize(&mut writer)?;

		self.prev_item = Some(current_pos);
		writer.flush()?;
		mem::drop(writer);

		self.size = self.file.seek(SeekFrom::End(0))? as usize;
		Ok(current_pos)
	}

	fn read_item_at(&mut self, offset: NonZeroU64) -> Result<Item<'static>, FileError> {
		debug_assert!(offset.get() >= self.body_start);

		self.file.seek(SeekFrom::Start(offset.get()))?;
		let mut reader = ItemReader::new(&mut self.file, None);
		let Some(item) = reader.read_item()? else {
			return Err(FileError::UnexpectedEof);
		};
		Ok(item)
	}

	fn iter_items(&mut self) -> Result<Self::IterItems<'_>, FileError> {
		self.file.seek(SeekFrom::Start(self.body_start))?;
		Ok(IterItems::new(&mut self.file))
	}

	fn iter_items_reverse(&mut self) -> Result<Self::IterItemsReverse<'_>, FileError> {
		self.file.seek(SeekFrom::End(0))?;
		Ok(IterItemsReverse::new(&mut self.file, self.prev_item))
	}

	#[inline]
	fn size(&self) -> usize {
		self.size
	}
}

struct ItemReader<F: Read + Seek> {
	reader: BufReader<F>,
	prev_item: Option<NonZeroU64>,
}

impl<F: Read + Seek> ItemReader<F> {
	fn new(file: F, prev_item: Option<NonZeroU64>) -> Self {
		Self {
			reader: BufReader::new(file),
			prev_item,
		}
	}

	fn read_transaction_data(body: impl Read) -> Result<TransactionData, FileError> {
		let transaction_block = TransactionBlock::deserialize(body)?;

		Ok(TransactionData {
			transaction_id: transaction_block.transaction_id,
			prev_transaction_item: transaction_block.prev_transaction_item,
		})
	}

	fn read_write_data(mut body: impl Read) -> Result<WriteData<'static>, FileError> {
		let transaction_data = Self::read_transaction_data(&mut body)?;

		let write_block = WriteBlock::deserialize(&mut body)?;
		let mut from: Vec<u8> = vec![0; write_block.write_length.into()];
		body.read_exact(&mut from)?;
		let mut to: Vec<u8> = vec![0; write_block.write_length.into()];
		body.read_exact(&mut to)?;

		Ok(WriteData {
			transaction_data,
			page_id: write_block.page_id,
			offset: write_block.offset,
			from: Cow::Owned(from),
			to: Cow::Owned(to),
		})
	}

	fn read_checkpoint_data(mut body: impl Read) -> Result<CheckpointData<'static>, FileError> {
		let checkpoint_block = CheckpointBlock::deserialize(&mut body)?;

		let mut dirty_pages: HashMap<PageId, WalIndex> = HashMap::new();
		for _ in 0..checkpoint_block.num_dirty_pages {
			let page_id = PageId::deserialize(&mut body)?;
			let wal_index = WalIndex::deserialize(&mut body)?;
			dirty_pages.insert(page_id, wal_index);
		}

		let mut transactions: HashMap<u64, WalIndex> = HashMap::new();
		for _ in 0..checkpoint_block.num_transactions {
			let mut tid_bytes = [0; 8];
			body.read_exact(&mut tid_bytes)?;
			let transaction_id = u64::from_ne_bytes(tid_bytes);
			let wal_index = WalIndex::deserialize(&mut body)?;
			transactions.insert(transaction_id, wal_index);
		}

		Ok(CheckpointData {
			dirty_pages: Cow::Owned(dirty_pages),
			transactions: Cow::Owned(transactions),
		})
	}

	fn read_item(&mut self) -> Result<Option<Item<'static>>, FileError> {
		if !self.reader.has_data_left()? {
			return Ok(None);
		}
		let header = ItemHeader::deserialize(&mut self.reader)?;
		let mut body_buf: Box<[u8]> = vec![0; header.body_length.into()].into();
		self.reader.read_exact(&mut body_buf)?;
		self.prev_item = header.prev_item;

		if CRC32.checksum(&body_buf) != header.crc {
			return Err(FileError::ChecksumMismatch);
		}

		let mut body_cursor = Cursor::new(body_buf);
		let item = match header.kind {
			ItemKind::Write => Item::Write(Self::read_write_data(&mut body_cursor)?),
			ItemKind::Commit => Item::Commit(Self::read_transaction_data(&mut body_cursor)?),
			ItemKind::Undo => Item::Undo(Self::read_transaction_data(&mut body_cursor)?),
			ItemKind::Checkpoint => Item::Checkpoint(Self::read_checkpoint_data(&mut body_cursor)?),
		};

		self.reader.seek_relative(ItemFooter::REPR_SIZE as i64)?;

		Ok(Some(item))
	}

	fn read_prev_item(&mut self) -> Result<Option<Item<'static>>, FileError> {
		let Some(prev_item) = self.prev_item else {
			return Ok(None);
		};
		self.reader.seek(SeekFrom::Start(prev_item.get()))?;
		self.read_item()
	}
}

pub(crate) struct IterItems<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> IterItems<F> {
	fn new(file: F) -> Self {
		Self {
			reader: ItemReader::new(file, None),
		}
	}
}

impl<F: Read + Seek> Iterator for IterItems<F> {
	type Item = Result<Item<'static>, FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_item().transpose()
	}
}

pub(crate) struct IterItemsReverse<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> IterItemsReverse<F> {
	fn new(file: F, prev_item: Option<NonZeroU64>) -> Self {
		Self {
			reader: ItemReader::new(file, prev_item),
		}
	}
}

impl<F: Read + Seek> Iterator for IterItemsReverse<F> {
	type Item = Result<Item<'static>, FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_prev_item().transpose()
	}
}

#[cfg(test)]
mod tests {
	use crate::files::generic::GenericHeaderRepr;

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
				content_offset: GenericHeader::REPR_SIZE as u16,
				version: FORMAT_VERSION,
			})
			.as_bytes(),
		);

		assert_eq!(file.len(), GenericHeader::REPR_SIZE);
		assert_eq!(file, expected_data);
	}

	#[test]
	fn open_wal() {
		// given
		let mut file = Vec::<u8>::new();
		file.extend(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Wal,
				content_offset: GenericHeader::REPR_SIZE as u16,
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
					prev_transaction_item: NonZeroU64::new(24),
				},
				page_id: 123,
				offset: 445,
				from: Cow::Owned(vec![1, 2, 3, 4]),
				to: Cow::Owned(vec![4, 5, 6, 7]),
			}))
			.unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Write as u8,
				_padding: 0,
				body_length: 36,
				crc: 0xcef5c9ba,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_item: std::num::NonZeroU64::new(24),
				transaction_id: 25,
			}
			.as_bytes(),
		);
		expected_body.extend(
			WriteBlockRepr {
				page_id: 123,
				offset: 445,
				write_length: 4,
			}
			.as_bytes(),
		);
		expected_body.extend([1, 2, 3, 4]);
		expected_body.extend([4, 5, 6, 7]);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeader::REPR_SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_eq!(file[GenericHeader::REPR_SIZE..], expected_body);
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
				prev_transaction_item: NonZeroU64::new(25),
			}))
			.unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Commit as u8,
				_padding: 0,
				body_length: 16,
				crc: 0xdb684ab9,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_item: std::num::NonZeroU64::new(25),
				transaction_id: 69,
			}
			.as_bytes(),
		);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeader::REPR_SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_eq!(file[GenericHeader::REPR_SIZE..], expected_body);
	}

	#[test]
	fn push_undo_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		wal_file
			.push_item(Item::Undo(TransactionData {
				transaction_id: 69,
				prev_transaction_item: NonZeroU64::new(25),
			}))
			.unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Undo as u8,
				_padding: 0,
				body_length: 16,
				crc: 0xdb684ab9,
				prev_item: NonZeroU64::new(0),
			}
			.as_bytes(),
		);
		expected_body.extend(
			TransactionBlockRepr {
				prev_transaction_item: std::num::NonZeroU64::new(25),
				transaction_id: 69,
			}
			.as_bytes(),
		);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeader::REPR_SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_eq!(file[GenericHeader::REPR_SIZE..], expected_body);
	}

	#[test]
	fn push_checkpoint_item() {
		// given
		let mut file = Vec::<u8>::new();
		let mut wal_file = WalFile::create(Cursor::new(&mut file)).unwrap();

		// when
		let mut dirty_pages = HashMap::new();
		dirty_pages.insert(
			PageId::new(1, NonZeroU16::new(2).unwrap()),
			WalIndex::new(0, 3),
		);
		let mut transactions = HashMap::new();
		transactions.insert(69, WalIndex::new(0, 420));
		wal_file
			.push_item(Item::Checkpoint(CheckpointData {
				dirty_pages: Cow::Borrowed(&dirty_pages),
				transactions: Cow::Borrowed(&transactions),
			}))
			.unwrap();

		// then
		let mut expected_body = Vec::<u8>::new();
		expected_body.extend(
			ItemHeaderRepr {
				kind: ItemKind::Checkpoint as u8,
				_padding: 0,
				body_length: 62,
				crc: 0xc3819119,
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
			PageIdRepr {
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
			WalIndexRepr {
				generation: 0,
				offset: 420,
			}
			.as_bytes(),
		);
		expected_body.extend(
			ItemFooterRepr {
				item_start: GenericHeader::REPR_SIZE as u64,
			}
			.as_bytes(),
		);

		assert_eq!(wal_file.size(), file.len());
		assert_eq!(file[GenericHeader::REPR_SIZE..], expected_body);
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
			page_id: 69,
			offset: 420,
			from: Cow::Owned(vec![0, 0, 0, 0]),
			to: Cow::Owned(vec![1, 2, 3, 4]),
		});

		// when
		let offset = wal_file.push_item(item.clone()).unwrap();

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
				page_id: 69,
				offset: 420,
				from: Cow::Owned(vec![0, 0, 0, 0]),
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

		// then
		let mut iter = wal_file.iter_items().unwrap();
		assert_eq!(iter.next().unwrap().unwrap(), items[0]);
		assert_eq!(iter.next().unwrap().unwrap(), items[1]);
		assert!(iter.next().is_none());
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
				page_id: 69,
				offset: 420,
				from: Cow::Owned(vec![0, 0, 0, 0]),
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

		// then
		let mut iter = wal_file.iter_items_reverse().unwrap();
		assert_eq!(iter.next().unwrap().unwrap(), items[1]);
		assert_eq!(iter.next().unwrap().unwrap(), items[0]);
		assert!(iter.next().is_none());
	}
}
