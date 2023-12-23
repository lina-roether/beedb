use std::{io, num::NonZeroU32, u64, usize};

use thiserror::Error;

use crate::utils::byte_order::ByteOrder;

use super::file::StorageFile;

#[derive(Debug, Error)]
pub enum Error {
	#[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
	NotAStorageFile,

	#[error("The format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("The storage is corrupted (Unexpected end of file)")]
	UnexpectedEOF,

	#[error("Failed to expand storage file")]
	IncompleteWrite,

	#[error("The storage file metadata is corrupted")]
	CorruptedMeta,

	#[error(transparent)]
	Io(#[from] io::Error),
}

const MAGIC: [u8; 4] = *b"ACRN";

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

#[derive(Debug, Clone)]
pub struct Meta {
	pub format_version: u8,
	pub page_size_exponent: u8,
	pub byte_order: ByteOrder,
}

impl Meta {
	const SIZE: usize = 32;

	pub fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
		let mut buffer: [u8; Self::SIZE] = Default::default();
		let bytes_read = file.read_at(&mut buffer, 0)?;

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

	pub fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&MAGIC);
		buf[4] = self.format_version;
		buf[5] = self.page_size_exponent;
		buf[6] = self.byte_order as u8;

		if file.write_at(&buf, 0)? != buf.len() {
			return Err(Error::IncompleteWrite);
		}
		Ok(())
	}
}

/*
 * State block layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The current size of the dababase in pages.                                 |
 * |      4 |    4 | The first page of the freelist.                                            |
 * |      8 |    4 | The current length of the freelist.                                        |
 * |     12 |   20 | Reserved for future use. Must be zero.                                     |
 *
 */

#[derive(Debug)]
pub struct State {
	pub num_pages: usize,
	pub freelist_trunk: Option<NonZeroU32>,
	pub freelist_length: usize,
}

impl State {
	const SIZE: usize = 32;
	const OFFSET: u64 = Meta::SIZE as u64;

	pub fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();
		if file.read_at(&mut buf, Self::OFFSET)? != buf.len() {
			return Err(Error::UnexpectedEOF);
		}

		let num_pages = u32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize;
		let freelist_trunk = NonZeroU32::new(u32::from_ne_bytes(buf[4..8].try_into().unwrap()));
		let freelist_length = u32::from_ne_bytes(buf[8..12].try_into().unwrap()) as usize;

		Ok(Self {
			num_pages,
			freelist_trunk,
			freelist_length,
		})
	}

	pub fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&(self.num_pages as u32).to_ne_bytes());
		buf[4..8].copy_from_slice(
			&self
				.freelist_trunk
				.map(NonZeroU32::get)
				.unwrap_or(0)
				.to_ne_bytes(),
		);
		buf[8..12].copy_from_slice(&(self.freelist_length as u32).to_ne_bytes());

		if file.write_at(&buf, Self::OFFSET)? != buf.len() {
			return Err(Error::IncompleteWrite);
		}

		Ok(())
	}
}

pub struct PageStorage<F: StorageFile> {
	page_size: usize,
	file: F,
}

impl<F: StorageFile> PageStorage<F> {
	pub fn new(file: F, page_size: usize) -> Self {
		Self { file, page_size }
	}

	pub fn read_page(&self, buf: &mut [u8], page_number: NonZeroU32) -> Result<(), Error> {
		debug_assert_eq!(buf.len(), self.page_size);

		let offset = self.page_offset(page_number);
		let bytes_read = self.file.read_at(&mut buf[0..self.page_size], offset)?;
		if bytes_read != self.page_size {
			return Err(Error::UnexpectedEOF);
		}

		Ok(())
	}

	pub fn write_page(&mut self, buf: &[u8], page_number: NonZeroU32) -> Result<(), Error> {
		debug_assert_eq!(buf.len(), self.page_size);

		let offset = self.page_offset(page_number);
		let bytes_written = self.file.write_at(&buf[0..self.page_size], offset)?;
		if bytes_written != self.page_size {
			return Err(Error::IncompleteWrite);
		}

		Ok(())
	}

	#[inline]
	pub fn page_size(&self) -> usize {
		self.page_size
	}

	fn page_offset(&self, page_number: NonZeroU32) -> u64 {
		u32::from(page_number) as u64 * self.page_size as u64
	}
}

#[cfg(test)]
mod tests {
	use std::{assert_matches::assert_matches, iter};

	use super::*;

	#[test]
	fn read_metadata() {
		let data = vec![
			0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		let meta = Meta::read_from(&data).unwrap();
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

		assert_matches!(Meta::read_from(&data), Err(Error::NotAStorageFile));
	}

	#[test]
	fn try_read_with_unsupported_version() {
		let data = vec![
			0x41, 0x43, 0x52, 0x4e, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		assert_matches!(Meta::read_from(&data), Err(Error::UnsupportedVersion(..)));
	}

	#[test]
	fn try_read_incomplete_magic() {
		let data = vec![0x41, 0x43];

		assert_matches!(Meta::read_from(&data), Err(Error::NotAStorageFile));
	}

	#[test]
	fn try_read_incomplete_meta() {
		let data = vec![0x41, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00];

		assert_matches!(Meta::read_from(&data), Err(Error::UnexpectedEOF));
	}

	#[test]
	fn write_metadata() {
		let mut data = Vec::new();

		let meta = Meta {
			format_version: 1,
			page_size_exponent: 10,
			byte_order: ByteOrder::Little,
		};

		meta.write_to(&mut data).unwrap();

		assert_eq!(
			data,
			vec![
				0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00
			]
		)
	}

	#[test]
	fn read_state() {
		let mut data: Vec<u8> = iter::repeat(0x00).take(Meta::SIZE).collect();
		data.extend(69_u32.to_ne_bytes());
		data.extend(420_u32.to_ne_bytes());
		data.extend(42069_u32.to_ne_bytes());
		data.extend(iter::repeat(0x00).take(20));

		let state = State::read_from(&data).unwrap();

		assert_eq!(state.num_pages, 69);
		assert_eq!(state.freelist_trunk, NonZeroU32::new(420));
		assert_eq!(state.freelist_length, 42069);
	}

	#[test]
	fn try_read_incomplete_state() {
		let data = vec![0x00, 0x00, 0x00];

		assert_matches!(State::read_from(&data), Err(Error::UnexpectedEOF));
	}

	#[test]
	fn write_state() {
		let mut data = Vec::<u8>::new();

		let state = State {
			num_pages: 123,
			freelist_trunk: NonZeroU32::new(543),
			freelist_length: 5432,
		};
		state.write_to(&mut data).unwrap();

		let mut expected: Vec<u8> = iter::repeat(0x00).take(Meta::SIZE).collect();
		expected.extend(123_u32.to_ne_bytes());
		expected.extend(543_u32.to_ne_bytes());
		expected.extend(5432_u32.to_ne_bytes());
		expected.extend(iter::repeat(0x00).take(20));

		assert_eq!(data, expected);
	}

	#[test]
	fn read_page() {
		let storage = PageStorage::new(vec![0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x56, 0x78], 4);

		let mut buf: [u8; 4] = Default::default();
		storage
			.read_page(&mut buf, NonZeroU32::new(1).unwrap())
			.unwrap();
		assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
	}

	#[test]
	fn write_page() {
		let mut storage = PageStorage::new(Vec::new(), 4);
		storage
			.write_page(&[0x23, 0x89, 0x43, 0x79], NonZeroU32::new(3).unwrap())
			.unwrap();

		assert_eq!(
			storage.file,
			vec![
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x89,
				0x43, 0x79,
			]
		)
	}
}
