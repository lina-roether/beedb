/*
 * Since the metadata contains the byte order, it is architecture-independent
 * and always big-endian.
 *
 * Metadata layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The magic bytes "ACRN" (hex: 0x41 0x43 0x52 0x4e)                          |
 * |      4 |    1 | The format version. Must be 1.                                             |
 * |      5 |    1 | The base 2 logarithm of the page size used in the file.                    |
 * |      6 |    1 | The byte order used in the file. 0 for big endian, 1 for little endian     |
 * |      7 |   25 | Reserved for future use. Must be zero.                                     |
 *
 */

use crate::{storage::StorageFile, utils::byte_order::ByteOrder};

use super::{Error, MAGIC};

#[derive(Debug, Clone)]
pub struct Meta {
	pub format_version: u8,
	pub page_size_exponent: u8,
	pub byte_order: ByteOrder,
}

impl Meta {
	pub const SIZE: usize = 32;

	pub fn read_from<const OFFSET: u64>(file: &impl StorageFile) -> Result<Self, Error> {
		let mut buffer: [u8; Self::SIZE] = Default::default();
		let bytes_read = file.read_at(&mut buffer, OFFSET)?;

		let magic: [u8; 4] = buffer[0..4].try_into().unwrap();
		if magic != MAGIC {
			return Err(Error::NotAStorageFile);
		}

		if bytes_read != buffer.len() {
			return Err(Error::UnexpectedEOF);
		}

		let format_version = buffer[4];
		if format_version != 1 {
			return Err(Error::UnsupportedVersion(format_version));
		}

		let page_size_exponent = buffer[5];
		let byte_order = match buffer[6] {
			0 => ByteOrder::Big,
			1 => ByteOrder::Little,
			_ => return Err(Error::CorruptedMeta),
		};

		Ok(Self {
			format_version,
			page_size_exponent,
			byte_order,
		})
	}

	pub fn write_to<const OFFSET: u64>(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&MAGIC);
		buf[4] = self.format_version;
		buf[5] = self.page_size_exponent;
		buf[6] = self.byte_order as u8;

		if file.write_at(&buf, OFFSET)? != buf.len() {
			return Err(Error::IncompleteWrite);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::assert_matches::assert_matches;

	use super::*;

	#[test]
	fn read_metadata() {
		let data = vec![
			0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		let meta = Meta::read_from::<0>(&data).unwrap();
		assert_eq!(meta.format_version, 1);
		assert_eq!(meta.page_size_exponent, 10);
		assert_eq!(meta.byte_order, ByteOrder::Little);
	}

	#[test]
	fn try_read_with_invalid_magic() {
		let data = vec![
			0x42, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		assert_matches!(Meta::read_from::<0>(&data), Err(Error::NotAStorageFile));
	}

	#[test]
	fn try_read_with_unsupported_version() {
		let data = vec![
			0x41, 0x43, 0x52, 0x4e, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		assert_matches!(
			Meta::read_from::<0>(&data),
			Err(Error::UnsupportedVersion(..))
		);
	}

	#[test]
	fn try_read_incomplete_magic() {
		let data = vec![0x41, 0x43];

		assert_matches!(Meta::read_from::<0>(&data), Err(Error::NotAStorageFile));
	}

	#[test]
	fn try_read_incomplete_meta() {
		let data = vec![0x41, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00];

		assert_matches!(Meta::read_from::<0>(&data), Err(Error::UnexpectedEOF));
	}

	#[test]
	fn write_metadata() {
		let mut data = Vec::new();

		let meta = Meta {
			format_version: 1,
			page_size_exponent: 10,
			byte_order: ByteOrder::Little,
		};

		meta.write_to::<0>(&mut data).unwrap();

		assert_eq!(
			data,
			vec![
				0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00
			]
		)
	}
}
