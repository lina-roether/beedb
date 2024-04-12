use std::{
	borrow::Borrow,
	io::{Read, Seek, SeekFrom, Write},
	mem::size_of,
};

use aligned::{Aligned, A2};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::{FileError, FileType, GenericHeader, GenericMeta};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
struct ItemHeader {
	item_type: u8,
	flags: u8,
	data_length: u16,
	crc: u32,
	transaction_id: u64,
	sequence_num: u64,
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
struct ItemFooter {
	start_offset: i16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsBytes, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub(super) enum ItemType {
	Write = 0,
	Commit = 1,
	Undo = 2,
}

#[derive(Debug)]
pub(super) struct ItemMeta {
	pub item_type: ItemType,
	pub data_length: u16,
	pub crc: u32,
	pub transaction_id: u64,
	pub sequence_num: u64,
}

impl From<&ItemMeta> for ItemHeader {
	fn from(value: &ItemMeta) -> Self {
		Self {
			item_type: value.item_type.into(),
			flags: 0,
			data_length: value.data_length,
			crc: value.crc,
			transaction_id: value.transaction_id,
			sequence_num: value.sequence_num,
		}
	}
}

impl TryFrom<&ItemHeader> for ItemMeta {
	type Error = FileError;

	fn try_from(value: &ItemHeader) -> Result<Self, Self::Error> {
		let item_type = ItemType::try_from(value.item_type).map_err(FileError::corrupted)?;
		Ok(Self {
			item_type,
			data_length: value.data_length,
			crc: value.crc,
			transaction_id: value.transaction_id,
			sequence_num: value.sequence_num,
		})
	}
}

pub(crate) struct WalFile<F: Seek + Read + Write> {
	body_start: u64,
	file: F,
}

impl<F: Seek + Read + Write> WalFile<F> {
	const BODY_START: u16 = size_of::<GenericHeader>() as u16;

	fn create(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let meta = GenericMeta {
			file_type: FileType::Wal,
			content_offset: Self::BODY_START,
		};
		file.write_all(GenericHeader::from(&meta).as_bytes())?;

		Ok(Self {
			file,
			body_start: Self::BODY_START.into(),
		})
	}

	fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let mut header_bytes: Aligned<A2, [u8; size_of::<GenericHeader>()]> = Default::default();
		file.read_exact(header_bytes.as_mut_slice())?;
		let meta = GenericMeta::try_from(GenericHeader::ref_from(header_bytes.borrow()).unwrap())?;
		if meta.file_type != FileType::Wal {
			return Err(FileError::WrongFileType(meta.file_type));
		}
		Ok(Self {
			body_start: meta.content_offset.into(),
			file,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::{borrow::Borrow, io::Cursor};

	use super::*;

	#[test]
	fn item_meta_from_header() {
		let item_header = ItemHeader {
			item_type: ItemType::Write.into(),
			flags: 0,
			data_length: 69,
			crc: 420,
			transaction_id: 25,
			sequence_num: 24,
		};
		let meta = ItemMeta::try_from(&item_header).unwrap();
		assert_eq!(meta.item_type, ItemType::Write);
		assert_eq!(meta.data_length, 69);
		assert_eq!(meta.crc, 420);
		assert_eq!(meta.transaction_id, 25);
		assert_eq!(meta.sequence_num, 24);
	}

	#[test]
	fn try_item_meta_from_header_corrupted_item_type() {
		let item_header = ItemHeader {
			item_type: 123,
			flags: 0,
			data_length: 69,
			crc: 420,
			transaction_id: 25,
			sequence_num: 24,
		};
		let err = ItemMeta::try_from(&item_header).unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file is corrupted: No discriminant in enum `ItemType` matches the value `123`"
		);
	}

	#[test]
	fn create_wal_file() {
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		WalFile::create(&mut file).unwrap();

		let mut aligned: Aligned<A2, [u8; size_of::<GenericHeader>()]> =
			Aligned(Default::default());
		aligned.copy_from_slice(&file.into_inner());
		let meta =
			GenericMeta::try_from(GenericHeader::ref_from(aligned.borrow()).unwrap()).unwrap();
		assert_eq!(meta.file_type, FileType::Wal);
		assert_eq!(meta.content_offset, size_of::<GenericHeader>() as u16);
	}
}
