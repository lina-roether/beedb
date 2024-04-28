use std::{io, mem::size_of};

use crc::Crc;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) mod wal;

pub(super) const CRC16: Crc<u16> = Crc::<u16>::new(&crc::CRC_16_ARC);

#[derive(Debug, Error)]
pub(crate) enum FileError {
	#[error("The file is not an acorn database file")]
	MissingMagic,

	#[error("The file was created on a platform with a different byte order and cannot be opened")]
	ByteOrderMismatch,

	#[error("The file is corrupted: {0}")]
	Corrupted(#[from] bincode::Error),

	#[error("Unexpected file type {0:?}")]
	WrongFileType(FileTypeRepr),

	#[error("Unexpected end of file")]
	UnexpectedEof,

	#[error("The file is corrupted; a checksum mismatch occurred")]
	ChecksumMismatch,

	#[error(transparent)]
	Io(#[from] io::Error),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) enum FileTypeRepr {
	Wal = 0,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
enum ByteOrderRepr {
	BigEndian = 0,
	LittleEndian = 1,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct GenericHeaderRepr {
	magic: [u8; 4],
	byte_order: ByteOrderRepr,
	file_type: FileTypeRepr,
	content_offset: u16,
}

const MAGIC: [u8; 4] = *b"ACRN";

#[cfg(target_endian = "big")]
const NATIVE_BYTE_ORDER: ByteOrder = ByteOrderRepr::BigEndian;

#[cfg(target_endian = "little")]
const NATIVE_BYTE_ORDER: ByteOrderRepr = ByteOrderRepr::LittleEndian;

#[derive(Debug)]
pub(super) struct GenericHeaderInit {
	file_type: FileTypeRepr,
	header_size: u16,
}

impl GenericHeaderRepr {
	pub fn new(
		GenericHeaderInit {
			file_type,
			header_size,
		}: GenericHeaderInit,
	) -> Self {
		Self {
			magic: MAGIC,
			byte_order: NATIVE_BYTE_ORDER,
			file_type,
			content_offset: (size_of::<Self>() as u16) + header_size,
		}
	}

	pub fn validate(&self) -> Result<(), FileError> {
		if self.magic != MAGIC {
			return Err(FileError::MissingMagic);
		}
		if self.byte_order != NATIVE_BYTE_ORDER {
			return Err(FileError::ByteOrderMismatch);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn verify_header() {
		let header = GenericHeaderRepr {
			magic: *b"ACRN",
			byte_order: NATIVE_BYTE_ORDER,
			file_type: FileTypeRepr::Wal,
			content_offset: 69,
		};
		assert!(header.validate().is_ok());
	}

	#[test]
	fn try_verify_header_with_missing_magic() {
		let header = GenericHeaderRepr {
			magic: *b"KEKW",
			byte_order: NATIVE_BYTE_ORDER,
			file_type: FileTypeRepr::Wal,
			content_offset: 69,
		};
		let err = header.validate().unwrap_err();
		assert_eq!(err.to_string(), "The file is not an acorn database file");
	}

	#[test]
	fn try_verify_header_with_byte_order_mismatch() {
		let header = GenericHeaderRepr {
			magic: *b"ACRN",
			byte_order: match NATIVE_BYTE_ORDER {
				ByteOrderRepr::BigEndian => ByteOrderRepr::LittleEndian,
				ByteOrderRepr::LittleEndian => ByteOrderRepr::BigEndian,
			},
			file_type: FileTypeRepr::Wal,
			content_offset: 69,
		};
		let err = header.validate().unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file was created on a platform with a different byte order and cannot be opened"
		);
	}
}
