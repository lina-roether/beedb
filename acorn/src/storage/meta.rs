use std::{
	io::{self, Read, Seek, SeekFrom, Write},
	mem::size_of,
};

use thiserror::Error;

use crate::{
	consts::{
		validate_page_size, PageSizeBoundsError, META_FORMAT_VERSION, META_MAGIC, PAGE_SIZE_RANGE,
	},
	utils::{byte_order::ByteOrder, byte_view::ByteView},
};

#[derive(Debug, Error)]
pub enum MetaError {
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

	#[error("Failed to initialize the data directory meta file: {0}")]
	Io(#[from] io::Error),
}

pub struct InitParams {
	page_size: u16,
}

pub struct StorageMetaFile<F: Seek + Read + Write> {
	buf: Vec<u8>,
	file: F,
}

impl<F: Seek + Read + Write> StorageMetaFile<F> {
	pub fn load(mut file: F) -> Result<Self, MetaError> {
		file.seek(SeekFrom::Start(0))?;
		let mut buf = Vec::new();
		file.read_to_end(&mut buf)?;
		let meta_file = Self { buf, file };
		let meta = meta_file.get();
		if meta.magic != META_MAGIC {
			return Err(MetaError::NotAMetaFile);
		}
		if meta.format_version != META_FORMAT_VERSION {
			return Err(MetaError::UnsupportedVersion(meta.format_version));
		}
		let Some(byte_order) = ByteOrder::from_byte(meta.byte_order) else {
			return Err(MetaError::Corrupted);
		};
		if byte_order != ByteOrder::NATIVE {
			return Err(MetaError::ByteOrderMismatch(byte_order));
		}
		validate_page_size(meta.page_size())?;
		Ok(meta_file)
	}

	pub fn init(file: &mut F, params: InitParams) -> Result<(), InitError> {
		validate_page_size(params.page_size)?;
		let page_size_exponent = params.page_size.ilog2() as u8;

		file.seek(SeekFrom::Start(0))?;
		let mut buf: [u8; size_of::<StorageMeta>()] = Default::default();
		let meta = StorageMeta::from_bytes_mut(&mut buf);
		*meta = StorageMeta {
			magic: META_MAGIC,
			format_version: META_FORMAT_VERSION,
			byte_order: ByteOrder::NATIVE as u8,
			page_size_exponent,
			num_clusters: 0,
		};

		Ok(())
	}

	#[inline]
	pub fn get(&self) -> &StorageMeta {
		StorageMeta::from_bytes(&self.buf)
	}

	#[inline]
	pub fn get_mut(&mut self) -> &mut StorageMeta {
		StorageMeta::from_bytes_mut(&mut self.buf)
	}

	pub fn flush(&mut self) -> Result<(), MetaError> {
		self.file.seek(SeekFrom::Start(0))?;
		self.file.write_all(&self.buf)?;
		self.file.flush()?;
		Ok(())
	}
}

#[repr(C)]
pub struct StorageMeta {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size_exponent: u8,
	pub num_clusters: u32,
}

impl StorageMeta {
	#[inline]
	pub fn page_size(&self) -> u16 {
		1_u16
			.checked_shl(self.page_size_exponent.into())
			.unwrap_or(*PAGE_SIZE_RANGE.end())
	}
}

unsafe impl ByteView for StorageMeta {}

#[cfg(test)]
mod tests {
	use std::io::Cursor;

	use crate::utils::units::KiB;

	use super::*;

	#[test]
	fn load() {
		let mut data = Vec::new();
		data.extend(*b"ACNM");
		data.push(1);
		data.push(ByteOrder::NATIVE as u8);
		data.push(14);
		data.push(0);
		data.extend(420_u32.to_ne_bytes());

		let meta_file = StorageMetaFile::load(Cursor::new(data)).unwrap();
		let meta = meta_file.get();
		assert_eq!(meta.format_version, 1);
		assert_eq!(meta.byte_order, ByteOrder::NATIVE as u8);
		assert_eq!(meta.page_size_exponent, 14);
		assert_eq!(meta.page_size(), 16 * KiB as u16);
		assert_eq!(meta.num_clusters, 420);
	}

	#[test]
	fn load_with_too_large_page_size_exponent() {
		let mut data = Vec::new();
		data.extend(*b"ACNM");
		data.push(1);
		data.push(ByteOrder::NATIVE as u8);
		data.push(69);
		data.push(0);
		data.extend(420_u32.to_ne_bytes());

		let meta_file = StorageMetaFile::load(Cursor::new(data)).unwrap();
		let meta = meta_file.get();
		assert_eq!(meta.page_size(), 32 * KiB as u16); // Should be the maximum
	}
}
