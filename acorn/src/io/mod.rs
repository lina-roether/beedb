use std::{cell::UnsafeCell, io, mem::size_of};
use thiserror::Error;

use crate::{
	io::format::HeaderPage,
	lock::{PageReadGuard, PageWriteGuard},
	utils::byte_view::ByteView,
};

use self::target::IoTarget;

mod format;
mod target;

const MAGIC: [u8; 4] = *b"ACRN";

#[derive(Debug, Error)]
pub enum StorageError {
	#[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
	NotAStorageFile,

	#[error("The format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("The storage is corrupted (Unexpected end of file)")]
	IncompleteRead,

	#[error("Failed to expand storage file")]
	IncompleteWrite,

	#[error("An error occurred accessing the storage file: {0}")]
	Io(#[from] io::Error),
}

pub struct StorageFile<T: IoTarget> {
	page_size: usize,
	target: UnsafeCell<T>,
}

impl<T: IoTarget> StorageFile<T> {
	#[inline]
	pub fn new(target: T) -> Result<Self, StorageError> {
		let mut buf: [u8; size_of::<HeaderPage>()] = Default::default();
		let bytes_read = target.read_at(&mut buf, 0)?;
		if bytes_read != buf.len() {
			return Err(StorageError::IncompleteRead);
		}
		let header = HeaderPage::from_bytes(&buf);

		Ok(Self {
			page_size: header.page_size(),
			target: UnsafeCell::new(target),
		})
	}

	#[inline]
	pub fn page_size(&self) -> usize {
		self.page_size
	}

	pub fn read_page(
		&self,
		buf: &mut [u8],
		page_guard: &PageReadGuard,
	) -> Result<(), StorageError> {
		let bytes_read = unsafe {
			(*self.target.get()).read_at(
				&mut buf[0..self.page_size()],
				self.offset_of(page_guard.page_number()),
			)?
		};
		if bytes_read != self.page_size() {
			return Err(StorageError::IncompleteRead);
		}
		Ok(())
	}

	pub fn write_page(&self, buf: &[u8], page_guard: &PageWriteGuard) -> Result<(), StorageError> {
		let bytes_written = unsafe {
			(*self.target.get()).write_at(
				&buf[0..self.page_size()],
				self.offset_of(page_guard.page_number()),
			)?
		};
		if bytes_written != self.page_size() {
			return Err(StorageError::IncompleteWrite);
		}
		Ok(())
	}

	fn offset_of(&self, page_number: u32) -> u64 {
		page_number as u64 * self.page_size() as u64
	}
}
