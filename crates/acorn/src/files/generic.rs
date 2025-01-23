use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::repr::Repr;

use super::FileError;

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C, packed)]
pub(super) struct GenericHeaderRepr {
	magic: [u8; 4],
	byte_order: u8,
	file_type: u8,
	content_offset: u16,
	version: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FileType {
	Wal = 0,
	Segment = 1,
}

impl TryFrom<u8> for FileType {
	type Error = FileError;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Wal),
			1 => Ok(Self::Segment),
			_ => Err(FileError::Corrupted(format!("Unknown file type {value}"))),
		}
	}
}

#[cfg(target_endian = "big")]
const NATIVE_BYTE_ORDER: u8 = 0;

#[cfg(target_endian = "little")]
const NATIVE_BYTE_ORDER: u8 = 1;

const MAGIC: [u8; 4] = *b"ACRN";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GenericHeader {
	pub file_type: FileType,
	pub content_offset: u16,
	pub version: u8,
}

impl From<GenericHeader> for GenericHeaderRepr {
	fn from(value: GenericHeader) -> Self {
		Self {
			magic: MAGIC,
			byte_order: NATIVE_BYTE_ORDER,
			file_type: value.file_type as u8,
			content_offset: value.content_offset,
			version: value.version,
		}
	}
}

impl TryFrom<GenericHeaderRepr> for GenericHeader {
	type Error = FileError;

	fn try_from(value: GenericHeaderRepr) -> Result<Self, Self::Error> {
		if value.magic != MAGIC {
			return Err(FileError::MissingMagic);
		}
		if value.byte_order != NATIVE_BYTE_ORDER {
			return Err(FileError::ByteOrderMismatch);
		}
		Ok(Self {
			file_type: value.file_type.try_into()?,
			content_offset: value.content_offset,
			version: value.version,
		})
	}
}

impl Repr<GenericHeader> for GenericHeaderRepr {
	type Error = FileError;
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn verify_header() {
		let header_repr = GenericHeaderRepr {
			magic: *b"ACRN",
			byte_order: NATIVE_BYTE_ORDER,
			file_type: FileType::Wal as u8,
			content_offset: 69,
			version: 1,
		};
		assert_eq!(
			GenericHeader::try_from(header_repr).unwrap(),
			GenericHeader {
				file_type: FileType::Wal,
				content_offset: 69,
				version: 1
			}
		);
	}

	#[test]
	fn try_verify_header_with_missing_magic() {
		let header_repr = GenericHeaderRepr {
			magic: *b"KEKW",
			byte_order: NATIVE_BYTE_ORDER,
			file_type: FileType::Wal as u8,
			content_offset: 69,
			version: 1,
		};
		let err = GenericHeader::try_from(header_repr).unwrap_err();
		assert_eq!(err.to_string(), "The file is not an acorn database file");
	}

	#[test]
	fn try_verify_header_with_byte_order_mismatch() {
		let header_repr = GenericHeaderRepr {
			magic: *b"ACRN",
			byte_order: !NATIVE_BYTE_ORDER,
			file_type: FileType::Wal as u8,
			content_offset: 69,
			version: 1,
		};
		let err = GenericHeader::try_from(header_repr).unwrap_err();
		assert_eq!(
			err.to_string(),
			"The file was created on a platform with a different byte order and cannot be opened"
		);
	}
}
