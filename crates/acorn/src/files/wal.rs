use std::{
	borrow::Cow,
	io::{BufRead, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
	mem::size_of,
	num::NonZeroU32,
};

use mockall::automock;
use serde::{Deserialize, Serialize};

use crate::{files::CRC32, model::PageId};

use super::{FileError, FileTypeRepr, GenericHeaderInit, GenericHeaderRepr};

#[derive(Debug, Serialize, Deserialize)]
#[repr(u8)]
enum ItemKindRepr {
	Write = 0,
	Commit = 1,
	Undo = 3,
	Checkpoint = 4,
}

const FLAG_BEGIN_TRANSACTION: u8 = 0b00000001;

#[derive(Debug, Serialize, Deserialize)]
struct ItemHeaderRepr {
	kind: ItemKindRepr,
	flags: u8,
	body_length: u16,
	crc: u32,
	prev_item: Option<NonZeroU32>,
	sequence_num: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ItemFooterRepr {
	item_start: NonZeroU32,
}

#[derive(Debug, Serialize, Deserialize)]
struct TransactionDataRepr {
	transaction_id: u64,
	prev_transaction_item: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteDataHeaderRepr {
	page_id: PageId,
	offset: u16,
	write_length: u16,
}

pub(crate) struct WalFile<F: Seek + Read + Write> {
	body_start: u64,
	prev_item: Option<NonZeroU32>,
	file: F,
}

impl<F: Seek + Read + Write> WalFile<F> {
	pub fn create(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let meta = GenericHeaderRepr::new(GenericHeaderInit {
			file_type: FileTypeRepr::Wal,
			header_size: 0,
		});
		bincode::serialize_into(&mut file, &meta)?;
		Self::new(file, meta.content_offset.into())
	}

	pub fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let header: GenericHeaderRepr = bincode::deserialize_from(&mut file)?;
		header.validate()?;
		if header.file_type != FileTypeRepr::Wal {
			return Err(FileError::WrongFileType(header.file_type));
		}

		Self::new(file, header.content_offset.into())
	}

	fn new(mut file: F, body_start: u64) -> Result<Self, FileError> {
		// FIXME: this might break, because the size of the bincode representation is
		// not guaranteed to match
		let end_pos = file.seek(SeekFrom::End(-(size_of::<ItemFooterRepr>() as i64)))?;
		let prev_item = if end_pos != body_start {
			let footer: ItemFooterRepr = bincode::deserialize_from(&mut file)?;
			Some(footer.item_start)
		} else {
			None
		};
		Ok(Self {
			body_start,
			file,
			prev_item,
		})
	}

	fn write_transaction_data(
		mut writer: impl Write,
		data: TransactionData,
	) -> Result<(), FileError> {
		bincode::serialize_into(
			&mut writer,
			&TransactionDataRepr {
				transaction_id: data.transaction_id,
				prev_transaction_item: data.prev_transaction_item,
			},
		)?;
		Ok(())
	}

	fn write_write_data(mut writer: impl Write, data: WriteData) -> Result<(), FileError> {
		assert_eq!(data.from.len(), data.to.len());

		Self::write_transaction_data(&mut writer, data.transaction_data)?;
		bincode::serialize_into(
			&mut writer,
			&WriteDataHeaderRepr {
				page_id: data.page_id,
				offset: data.offset,
				write_length: data
					.from
					.len()
					.try_into()
					.expect("Write length must be 16 bit!"),
			},
		)?;
		writer.write_all(&data.from)?;
		writer.write_all(&data.to)?;
		Ok(())
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionData {
	pub transaction_id: u64,
	pub prev_transaction_item: u32,
	pub begins_transaction: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteData<'a> {
	pub transaction_data: TransactionData,
	pub page_id: PageId,
	pub offset: u16,
	pub from: Cow<'a, [u8]>,
	pub to: Cow<'a, [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ItemData<'a> {
	Write(WriteData<'a>),
	Commit(TransactionData),
	Undo(TransactionData),
	Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item<'a> {
	pub sequence_num: u64,
	pub data: ItemData<'a>,
}

#[automock(
    type ReadItems<'a> = std::vec::IntoIter<Result<Item<'static>, FileError>>;
    type ReadItemsReverse<'a> = std::vec::IntoIter<Result<Item<'static>, FileError>>;
)]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	type ReadItems<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;
	type ReadItemsReverse<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;

	fn push_item<'a>(&mut self, item: Item<'a>) -> Result<(), FileError>;
	fn read_items<'a>(&'a mut self) -> Result<Self::ReadItems<'a>, FileError>;
	fn read_items_reverse<'a>(&'a mut self) -> Result<Self::ReadItemsReverse<'a>, FileError>;
}

impl<F: Seek + Read + Write> WalFileApi for WalFile<F> {
	type ReadItems<'a> = ReadItems<&'a mut F> where F: 'a;
	type ReadItemsReverse<'a> = ReadItemsReverse<&'a mut F> where F: 'a;

	fn push_item(&mut self, item: Item<'_>) -> Result<(), FileError> {
		let current_pos = self.file.seek(SeekFrom::End(0))?;
		let mut writer = BufWriter::new(&mut self.file);

		let mut body_buffer: Vec<u8> = vec![];
		let kind: ItemKindRepr;
		let mut flags: u8 = 0;
		match item.data {
			ItemData::Write(write_data) => {
				kind = ItemKindRepr::Write;
				Self::write_write_data(&mut body_buffer, write_data)?;
			}
			ItemData::Commit(transaction_data) => {
				kind = ItemKindRepr::Commit;
				if transaction_data.begins_transaction {
					flags |= FLAG_BEGIN_TRANSACTION;
				}
				Self::write_transaction_data(&mut body_buffer, transaction_data)?
			}
			ItemData::Undo(transaction_data) => {
				kind = ItemKindRepr::Undo;
				Self::write_transaction_data(&mut body_buffer, transaction_data)?
			}
			ItemData::Checkpoint => {
				kind = ItemKindRepr::Checkpoint;
			}
		};
		let crc = CRC32.checksum(&body_buffer);

		bincode::serialize_into(
			&mut writer,
			&ItemHeaderRepr {
				kind,
				flags,
				body_length: body_buffer
					.len()
					.try_into()
					.expect("Body length must be 16-bit!"),
				crc,
				prev_item: self.prev_item,
				sequence_num: item.sequence_num,
			},
		)?;
		self.prev_item = Some(
			NonZeroU32::new(current_pos as u32).expect("Cannot write log entries at position 0"),
		);

		Ok(())
	}

	fn read_items(&mut self) -> Result<Self::ReadItems<'_>, FileError> {
		self.file.seek(SeekFrom::Start(self.body_start))?;
		Ok(ReadItems::new(&mut self.file))
	}

	fn read_items_reverse(&mut self) -> Result<Self::ReadItemsReverse<'_>, FileError> {
		self.file.seek(SeekFrom::End(0))?;
		Ok(ReadItemsReverse::new(&mut self.file, self.prev_item))
	}
}

struct ItemReader<F: Read + Seek> {
	reader: BufReader<F>,
	prev_item: Option<NonZeroU32>,
}

impl<F: Read + Seek> ItemReader<F> {
	fn new(file: F, prev_item: Option<NonZeroU32>) -> Self {
		Self {
			reader: BufReader::new(file),
			prev_item,
		}
	}

	fn read_transaction_data(mut body: impl Read, flags: u8) -> Result<TransactionData, FileError> {
		let begins_transaction = (flags & FLAG_BEGIN_TRANSACTION) != 0;
		let transaction_data: TransactionDataRepr = bincode::deserialize_from(&mut body)?;

		Ok(TransactionData {
			transaction_id: transaction_data.transaction_id,
			prev_transaction_item: transaction_data.prev_transaction_item,
			begins_transaction,
		})
	}

	fn read_write_data(mut body: impl Read, flags: u8) -> Result<WriteData<'static>, FileError> {
		let transaction_data = Self::read_transaction_data(&mut body, flags)?;

		let write_header: WriteDataHeaderRepr = bincode::deserialize_from(&mut body)?;
		let mut from: Vec<u8> = vec![0; write_header.write_length.into()];
		body.read_exact(&mut from)?;
		let mut to: Vec<u8> = vec![0; write_header.write_length.into()];
		body.read_exact(&mut to)?;

		Ok(WriteData {
			transaction_data,
			page_id: write_header.page_id,
			offset: write_header.offset,
			from: Cow::Owned(from),
			to: Cow::Owned(to),
		})
	}

	fn read_item(&mut self) -> Result<Option<Item<'static>>, FileError> {
		if !self.reader.has_data_left()? {
			return Ok(None);
		}
		let header: ItemHeaderRepr = bincode::deserialize_from(&mut self.reader)?;
		let mut body_buf: Box<[u8]> = vec![0; header.body_length.into()].into();
		self.reader.read_exact(&mut body_buf)?;
		self.prev_item = header.prev_item;

		if CRC32.checksum(&body_buf) != header.crc {
			return Err(FileError::ChecksumMismatch);
		}

		let mut body_cursor = Cursor::new(body_buf);
		let data = match header.kind {
			ItemKindRepr::Write => {
				ItemData::Write(Self::read_write_data(&mut body_cursor, header.flags)?)
			}
			ItemKindRepr::Commit => {
				ItemData::Commit(Self::read_transaction_data(&mut body_cursor, header.flags)?)
			}
			ItemKindRepr::Undo => {
				ItemData::Undo(Self::read_transaction_data(&mut body_cursor, header.flags)?)
			}
			ItemKindRepr::Checkpoint => ItemData::Checkpoint,
		};

		let _: ItemFooterRepr = bincode::deserialize_from(&mut self.reader)?;

		Ok(Some(Item {
			data,
			sequence_num: header.sequence_num,
		}))
	}

	fn read_prev_item(&mut self) -> Result<Option<Item<'static>>, FileError> {
		let Some(prev_item) = self.prev_item else {
			return Ok(None);
		};
		self.reader.seek(SeekFrom::Start(prev_item.get().into()))?;
		self.read_item()
	}
}

pub(crate) struct ReadItems<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> ReadItems<F> {
	fn new(file: F) -> Self {
		Self {
			reader: ItemReader::new(file, None),
		}
	}
}

impl<F: Read + Seek> Iterator for ReadItems<F> {
	type Item = Result<Item<'static>, FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_item().transpose()
	}
}

pub(crate) struct ReadItemsReverse<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> ReadItemsReverse<F> {
	fn new(file: F, prev_item: Option<NonZeroU32>) -> Self {
		Self {
			reader: ItemReader::new(file, prev_item),
		}
	}
}

impl<F: Read + Seek> Iterator for ReadItemsReverse<F> {
	type Item = Result<Item<'static>, FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_prev_item().transpose()
	}
}
