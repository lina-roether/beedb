use std::{
	io::{self},
	mem::size_of,
};

use thiserror::Error;

use crate::{
	consts::{
		validate_page_size, PageSizeBoundsError, DEFAULT_PAGE_SIZE, META_FORMAT_VERSION,
		META_MAGIC, PAGE_SIZE_RANGE,
	},
	io::IoTarget,
	utils::{
		byte_order::ByteOrder,
		byte_view::{AlignedBytes, ByteView},
	},
};

#[derive(Debug, Error)]
pub enum LoadError {
	#[error(
		"The provided file is not a storage meta file (expected magic bytes {META_MAGIC:08x?})"
	)]
	NotAMetaFile,

	#[error("Meta format version {0} is not supported by this version of acorn")]
	UnsupportedVersion(u8),

	#[error("Cannot open a {0} storage on a {} device", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error("Cannot open a storage file with invalid configured page size: {0}")]
	PageSizeBounds(#[from] PageSizeBoundsError),

	#[error("The storage metadata is corrupted")]
	Corrupted,

	#[error("An error occurred accessing the data directory meta file: {0}")]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error(transparent)]
	PageSizeBounds(#[from] PageSizeBoundsError),

	#[error(transparent)]
	Io(#[from] io::Error),
}

pub struct InitParams {
	pub page_size: u16,
}

impl Default for InitParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

/*
 * TODO: Maybe this should just mmap() the file?
 */

pub struct StorageMetaFile<F: IoTarget> {
	buf: Box<AlignedBytes<12>>,
	file: F,
}

impl<F: IoTarget> StorageMetaFile<F> {
	pub fn load(file: F) -> Result<Self, LoadError> {
		let mut buf: [u8; size_of::<StorageMeta>()] = Default::default();
		if file.read_at(&mut buf, 0)? != buf.len() {
			return Err(LoadError::NotAMetaFile);
		}
		let meta_file = Self {
			buf: Box::new(AlignedBytes::from(buf)),
			file,
		};
		let meta = meta_file.get();
		if meta.magic != META_MAGIC {
			return Err(LoadError::NotAMetaFile);
		}
		if meta.format_version != META_FORMAT_VERSION {
			return Err(LoadError::UnsupportedVersion(meta.format_version));
		}
		let Some(byte_order) = ByteOrder::from_byte(meta.byte_order) else {
			return Err(LoadError::Corrupted);
		};
		if byte_order != ByteOrder::NATIVE {
			return Err(LoadError::ByteOrderMismatch(byte_order));
		}
		validate_page_size(meta.page_size())?;
		Ok(meta_file)
	}

	pub fn init(file: &mut F, params: InitParams) -> Result<(), InitError> {
		validate_page_size(params.page_size)?;
		let page_size_exponent = params.page_size.ilog2() as u8;

		let mut buf: AlignedBytes<12> = Default::default();
		let meta = StorageMeta::from_bytes_mut(buf.as_mut());
		*meta = StorageMeta {
			magic: META_MAGIC,
			format_version: META_FORMAT_VERSION,
			byte_order: ByteOrder::NATIVE as u8,
			page_size_exponent,
			num_segments: 0,
		};

		file.set_len(0)?;
		file.write_at(buf.as_ref(), 0)?;

		Ok(())
	}

	#[inline]
	pub fn get(&self) -> &StorageMeta {
		StorageMeta::from_bytes(&**self.buf)
	}

	#[inline]
	pub fn get_mut(&mut self) -> &mut StorageMeta {
		StorageMeta::from_bytes_mut(&mut **self.buf)
	}

	pub fn flush(&mut self) -> Result<(), io::Error> {
		self.file.set_len(0)?;
		self.file.write_at(&**self.buf, 0)?;
		Ok(())
	}
}

#[repr(C)]
pub struct StorageMeta {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size_exponent: u8,
	pub num_segments: u32,
}

impl StorageMeta {
	#[inline]
	pub fn page_size(&self) -> u16 {
		1_u16
			.checked_shl(self.page_size_exponent.into())
			.unwrap_or(*PAGE_SIZE_RANGE.end())
	}
}

// Safety: No fields in StorageMeta have internal invariants
unsafe impl ByteView for StorageMeta {}

#[cfg(test)]
mod tests {
	use crate::utils::{byte_view::AlignedBuffer, units::KiB};

	use super::*;

	#[test]
	fn load() {
		let mut data = AlignedBuffer::with_capacity(8, size_of::<StorageMeta>());
		data[0..4].copy_from_slice(b"ACNM");
		data[4] = 1;
		data[5] = ByteOrder::NATIVE as u8;
		data[6] = 14;
		data[7] = 0;
		data[8..12].copy_from_slice(&420_u32.to_ne_bytes());

		let meta_file = StorageMetaFile::load(data).unwrap();
		let meta = meta_file.get();
		assert_eq!(meta.format_version, 1);
		assert_eq!(meta.byte_order, ByteOrder::NATIVE as u8);
		assert_eq!(meta.page_size_exponent, 14);
		assert_eq!(meta.page_size(), 16 * KiB as u16);
		assert_eq!(meta.num_segments, 420);
	}

	#[test]
	fn load_with_too_large_page_size_exponent() {
		let mut data = AlignedBuffer::with_capacity(8, size_of::<StorageMeta>());
		data[0..4].copy_from_slice(b"ACNM");
		data[4] = 1;
		data[5] = ByteOrder::NATIVE as u8;
		data[6] = 69;
		data[7] = 0;
		data[8..12].copy_from_slice(&420_u32.to_ne_bytes());

		let meta_file = StorageMetaFile::load(data).unwrap();
		let meta = meta_file.get();
		assert_eq!(meta.page_size(), 32 * KiB as u16); // Should be the maximum
	}

	#[test]
	fn write_and_flush() {
		let mut data = AlignedBuffer::with_capacity(8, size_of::<StorageMeta>());
		data[0..4].copy_from_slice(b"ACNM");
		data[4] = 1;
		data[5] = ByteOrder::NATIVE as u8;
		data[6] = 14;
		data[7] = 0;
		data[8..12].copy_from_slice(&420_u32.to_ne_bytes());

		let mut meta_file = StorageMetaFile::load(data).unwrap();
		let meta = meta_file.get_mut();
		meta.num_segments = 69;

		assert_eq!(meta_file.file[8..12], 420_u32.to_ne_bytes());

		meta_file.flush().unwrap();

		assert_eq!(meta_file.file[8..12], 69_u32.to_ne_bytes());
	}
}
