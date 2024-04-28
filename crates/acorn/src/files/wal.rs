use std::{
	borrow::Cow,
	io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom, Write},
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
		Ok(Self::new(file, meta.content_offset.into()))
	}

	pub fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let header: GenericHeaderRepr = bincode::deserialize_from(&mut file)?;
		header.validate()?;
		if header.file_type != FileTypeRepr::Wal {
			return Err(FileError::WrongFileType(header.file_type));
		}

		Ok(Self::new(file, header.content_offset.into()))
	}

	fn new(file: F, body_start: u64) -> Self {
		Self { body_start, file }
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
    type RetraceTransaction<'a> = std::vec::IntoIter<Result<Item<'static>, FileError>>;
)]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	type ReadItems<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;
	type RetraceTransaction<'a>: Iterator<Item = Result<Item<'static>, FileError>> + 'a
	where
		Self: 'a;

	fn push_item<'a>(&mut self, item: Item<'a>) -> Result<(), FileError>;
	fn read_items<'a>(&'a mut self) -> Self::ReadItems<'a>;
	fn retrace_transaction<'a>(&'a mut self) -> Self::RetraceTransaction<'a>;
}

struct ItemReader<F: Read + Seek> {
	reader: BufReader<F>,
}

impl<F: Read + Seek> ItemReader<F> {
	fn new(file: F) -> Self {
		Self {
			reader: BufReader::new(file),
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

		Ok(Some(Item {
			data,
			sequence_num: header.sequence_num,
		}))
	}
}

struct ReadItems<F: Read + Seek> {
	reader: ItemReader<F>,
}

impl<F: Read + Seek> ReadItems<F> {
	fn new(file: F) -> Self {
		Self {
			reader: ItemReader::new(file),
		}
	}
}

impl<F: Read + Seek> Iterator for ReadItems<F> {
	type Item = Result<Item<'static>, FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.reader.read_item().transpose()
	}
}
