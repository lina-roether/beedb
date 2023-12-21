use std::{io, u64, usize};

use thiserror::Error;

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

	#[error(transparent)]
	Io(#[from] io::Error),
}

const CURRENT_VERSION: u8 = 1;
const MAGIC: [u8; 4] = *b"ACRN";

/*
 * Metadata layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The magic bytes "ACRN" (hex: 0x41 0x43 0x52 0x4e)                          |
 * |      4 |    1 | The format version. Must be 1.                                             |
 * |      5 |    1 | The base 2 logarithm of the page size used in the file (big-endian).       |
 * |      6 |   26 | Reserved for future use. Must be zero.                                     |
 *
 */

#[derive(Debug, Clone)]
pub struct Meta {
	pub format_version: u8,
	pub page_size_exponent: u8,
	pub page_size: usize,
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

		let page_size_exponent = u8::from_be_bytes(buffer[5..6].try_into().unwrap());

		Ok(Self {
			format_version,
			page_size_exponent,
			page_size: 1 << page_size_exponent,
		})
	}

	pub fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&MAGIC);
		buf[4] = self.format_version;
		buf[5..6].copy_from_slice(&self.page_size_exponent.to_be_bytes());

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
	pub num_pages: u32,
	pub freelist_trunk: u32,
	pub freelist_length: u32,
}

impl State {
	const SIZE: usize = 32;
	const OFFSET: u64 = Meta::SIZE as u64;

	pub fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();
		if file.read_at(&mut buf, Self::OFFSET)? != buf.len() {
			return Err(Error::UnexpectedEOF);
		}

		let num_pages = u32::from_be_bytes(buf[0..4].try_into().unwrap());
		let freelist_trunk = u32::from_be_bytes(buf[4..8].try_into().unwrap());
		let freelist_length = u32::from_be_bytes(buf[8..12].try_into().unwrap());

		Ok(Self {
			num_pages,
			freelist_trunk,
			freelist_length,
		})
	}

	pub fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&self.num_pages.to_be_bytes());
		buf[4..8].copy_from_slice(&self.freelist_trunk.to_be_bytes());
		buf[8..12].copy_from_slice(&self.freelist_length.to_be_bytes());

		if file.write_at(&buf, Self::OFFSET)? != buf.len() {
			return Err(Error::IncompleteWrite);
		}

		Ok(())
	}
}

const PAGE_SECTION_OFFSET: u64 = Meta::SIZE as u64 + State::SIZE as u64;

pub struct PageStorage<F: StorageFile> {
	page_size: usize,
	file: F,
}

impl<F: StorageFile> PageStorage<F> {
	pub fn new(file: F, page_size: usize) -> Self {
		Self { file, page_size }
	}

	pub fn read_page(&self, buf: &mut [u8], page_number: u32) -> Result<(), Error> {
		debug_assert_eq!(buf.len(), self.page_size);

		let offset = self.page_offset(page_number);
		let bytes_read = self.file.read_at(&mut buf[0..self.page_size], offset)?;
		if bytes_read != self.page_size {
			return Err(Error::UnexpectedEOF);
		}

		Ok(())
	}

	pub fn write_page(&mut self, buf: &[u8], page_number: u32) -> Result<(), Error> {
		debug_assert_eq!(buf.len(), self.page_size);

		let offset = self.page_offset(page_number);
		let bytes_written = self.file.write_at(&buf[0..self.page_size], offset)?;
		if bytes_written != self.page_size {
			return Err(Error::IncompleteWrite);
		}

		Ok(())
	}

	fn page_offset(&self, page_number: u32) -> u64 {
		PAGE_SECTION_OFFSET + (page_number as u64) * (self.page_size as u64)
	}
}

#[cfg(test)]
mod tests {
	use std::assert_matches::assert_matches;

	use super::*;

	#[test]
	fn read_metadata() {
		let data = vec![
			0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		let meta = Meta::read_from(&data).unwrap();
		assert_eq!(meta.format_version, 1);
		assert_eq!(meta.page_size, 1024);
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
			page_size: 1024,
		};

		meta.write_to(&mut data).unwrap();

		assert_eq!(
			data,
			vec![
				0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00
			]
		)
	}

	#[test]
	fn read_state() {
		let data = vec![
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x01, 0xa4, 0x00, 0x00,
			0xa4, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		];

		let state = State::read_from(&data).unwrap();

		assert_eq!(state.num_pages, 69);
		assert_eq!(state.freelist_trunk, 420);
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
			freelist_trunk: 543,
			freelist_length: 5432,
		};
		state.write_to(&mut data).unwrap();

		assert_eq!(
			data,
			vec![
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x02, 0x1f, 0x00, 0x00,
				0x15, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
			]
		);
	}

	#[test]
	fn read_page() {
		let storage = PageStorage::new(
			vec![
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x34,
				0x56, 0x78,
			],
			4,
		);

		let mut buf: [u8; 4] = Default::default();
		storage.read_page(&mut buf, 1).unwrap();
		assert_eq!(buf, [0x12, 0x34, 0x56, 0x78]);
	}

	#[test]
	fn write_page() {
		let mut storage = PageStorage::new(Vec::new(), 4);
		storage.write_page(&[0x23, 0x89, 0x43, 0x79], 3).unwrap();

		assert_eq!(
			storage.file,
			vec![
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x23, 0x89, 0x43, 0x79,
			]
		)
	}
}
