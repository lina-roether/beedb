use std::{error::Error, io};

use num_enum::{IntoPrimitive, TryFromPrimitive};
use thiserror::Error;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

pub(crate) mod wal;

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C, align(2))]
pub(super) struct GenericHeader {
	magic: [u8; 4],
	byte_order: u8,
	file_type: u8,
	content_offset: u16,
}

const MAGIC: [u8; 4] = *b"ACRN";

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsBytes, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub(super) enum FileType {
	Wal = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsBytes, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum ByteOrder {
	BigEndian = 0,
	LittleEndian = 1,
}

#[cfg(target_endian = "big")]
const NATIVE_BYTE_ORDER: ByteOrder = ByteOrder::BigEndian;

#[cfg(target_endian = "little")]
const NATIVE_BYTE_ORDER: ByteOrder = ByteOrder::LittleEndian;

#[derive(Debug, Error)]
pub(crate) enum FileError {
	#[error("The file is not an acorn database file")]
	MissingMagic,

	#[error("The file was created on a platform with a different byte order and cannot be opened")]
	ByteOrderMismatch,

	#[error("The file is corrupted: {0}")]
	Corrupted(#[source] Box<dyn Error>),

	#[error("Unexpected file type {0:?}")]
	WrongFileType(FileType),

	#[error(transparent)]
	Io(#[from] io::Error),
}

impl FileError {
	fn corrupted(source: impl Error + 'static) -> Self {
		Self::Corrupted(Box::new(source))
	}
}

#[derive(Debug)]
pub(super) struct GenericMeta {
	pub file_type: FileType,
	pub content_offset: u16,
}

impl From<&GenericMeta> for GenericHeader {
	fn from(value: &GenericMeta) -> Self {
		Self {
			magic: MAGIC,
			byte_order: NATIVE_BYTE_ORDER.into(),
			file_type: value.file_type.into(),
			content_offset: value.content_offset,
		}
	}
}

impl TryFrom<&GenericHeader> for GenericMeta {
	type Error = FileError;

	fn try_from(value: &GenericHeader) -> Result<Self, Self::Error> {
		if value.magic != MAGIC {
			return Err(FileError::MissingMagic);
		}
		let byte_order = ByteOrder::try_from(value.byte_order).map_err(FileError::corrupted)?;
		if byte_order != NATIVE_BYTE_ORDER {
			return Err(FileError::ByteOrderMismatch);
		}
		let file_type = FileType::try_from(value.file_type).map_err(FileError::corrupted)?;
		Ok(Self {
			file_type,
			content_offset: value.content_offset,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn meta_from_header() {
		let header = GenericHeader {
			magic: *b"ACRN",
			byte_order: NATIVE_BYTE_ORDER.into(),
			file_type: FileType::Wal.into(),
			content_offset: 69,
		};
		let meta = GenericMeta::try_from(&header).unwrap();
		assert_eq!(meta.file_type, FileType::Wal);
		assert_eq!(meta.content_offset, 69);
	}

	#[test]
	fn try_meta_from_header_missing_magic() {
		let header = GenericHeader {
			magic: *b"KEKW",
			byte_order: NATIVE_BYTE_ORDER.into(),
			file_type: FileType::Wal.into(),
			content_offset: 69,
		};
		let err = GenericMeta::try_from(&header).unwrap_err();
		assert_eq!(err.to_string(), "The file is not an acorn database file");
	}

	#[test]
	fn try_meta_from_header_byte_order_mismatch() {
		let header = GenericHeader {
			magic: *b"ACRN",
			byte_order: match NATIVE_BYTE_ORDER {
				ByteOrder::BigEndian => ByteOrder::LittleEndian.into(),
				ByteOrder::LittleEndian => ByteOrder::BigEndian.into(),
			},
			file_type: FileType::Wal.into(),
			content_offset: 69,
		};
		let err = GenericMeta::try_from(&header).unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file was created on a platform with a different byte order and cannot be opened"
		);
	}

	#[test]
	fn try_meta_from_header_corrupted_byte_order() {
		let header = GenericHeader {
			magic: *b"ACRN",
			byte_order: 123,
			file_type: FileType::Wal.into(),
			content_offset: 69,
		};
		let err = GenericMeta::try_from(&header).unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file is corrupted: No discriminant in enum `ByteOrder` matches the value `123`"
		);
	}

	#[test]
	fn try_meta_from_header_corrupted_file_type() {
		let header = GenericHeader {
			magic: *b"ACRN",
			byte_order: NATIVE_BYTE_ORDER.into(),
			file_type: 123,
			content_offset: 69,
		};
		let err = GenericMeta::try_from(&header).unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file is corrupted: No discriminant in enum `FileType` matches the value `123`"
		);
	}
}
