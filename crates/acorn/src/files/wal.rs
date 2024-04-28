use std::{
	borrow::Cow,
	io::{BufReader, Read, Seek, SeekFrom, Write},
	num::NonZeroU32,
};

use mockall::automock;
use serde::{Deserialize, Serialize};

use crate::model::PageId;

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
