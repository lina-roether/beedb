use parking_lot::{Mutex, RwLock};
use std::{cell::UnsafeCell, io, iter, mem::size_of, num::NonZeroU32, usize};
use thiserror::Error;

use crate::{
	io::format::{FreelistPageHeader, HeaderPage},
	lock::PageLocker,
	utils::{
		byte_order::ByteOrder,
		byte_view::ByteView,
		units::{KiB, B},
	},
};

use self::{format::FreelistPage, target::IoTarget};

mod format;
mod target;

#[derive(Debug, Error)]
pub enum StorageError {
	#[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
	NotAStorageFile,

	#[error("The format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("Cannot open a {0} storage file on a {} system", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error("The storage is corrupted (Unexpected end of file)")]
	IncompleteRead,

	#[error("Failed to expand storage file")]
	IncompleteWrite,

	#[error("The storage file is corrupted")]
	Corrupted,

	#[error("An error occurred accessing the storage file: {0}")]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Failed to write the complete file header")]
	IncompleteWrite,

	#[error("Page size {0} is invalid; must be a power of two and at least {MIN_PAGE_SIZE} B")]
	InvalidPageSize(usize),

	#[error("An error occurred initializing the storage file: {0}")]
	Io(#[from] io::Error),
}

pub const MAGIC: [u8; 4] = *b"ACRN";
pub const FORMAT_VERSION: u8 = 1;
pub const MIN_PAGE_SIZE: usize = 512 * B;
pub const DEFAULT_PAGE_SIZE: usize = 16 * KiB;

pub struct InitParams {
	pub page_size: usize,
}

impl Default for InitParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

pub struct StorageFile<T: IoTarget> {
	header_buf: RwLock<Box<[u8]>>,
	freelist_cache: Mutex<FreelistCache>,
	page_size: usize,
	target: UnsafeCell<T>,
	locker: PageLocker,
}

impl<T: IoTarget> StorageFile<T> {
	pub fn init(target: &mut T, params: InitParams) -> Result<(), InitError> {
		if !params.page_size.is_power_of_two() || params.page_size < MIN_PAGE_SIZE {
			return Err(InitError::InvalidPageSize(params.page_size));
		}

		let page_size_exponent = params.page_size.ilog2() as u8;
		let mut header_buf: [u8; size_of::<HeaderPage>()] = Default::default();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		*header = HeaderPage {
			magic: MAGIC,
			format_version: FORMAT_VERSION,
			page_size_exponent,
			byte_order: ByteOrder::NATIVE as u8,
			freelist_trunk: None,
			num_pages: 1,
		};
		if target.write_at(&header_buf, 0)? != header_buf.len() {
			return Err(InitError::IncompleteWrite);
		}
		Ok(())
	}

	pub fn load(target: T) -> Result<Self, StorageError> {
		let mut buf: [u8; size_of::<HeaderPage>()] = Default::default();
		let bytes_read = target.read_at(&mut buf, 0)?;
		if bytes_read != buf.len() {
			return Err(StorageError::IncompleteRead);
		}
		let header = HeaderPage::from_bytes(&buf);
		Self::validate_header(header)?;

		let page_size = 1 << header.page_size_exponent;

		let mut header_buf: Box<[u8]> = iter::repeat(0).take(page_size).collect();
		header_buf[0..size_of::<HeaderPage>()].copy_from_slice(&buf);

		let storage_file = Self {
			header_buf: RwLock::new(header_buf),
			freelist_cache: Mutex::new(FreelistCache::new(page_size)),
			page_size,
			target: UnsafeCell::new(target),
			locker: PageLocker::new(),
		};
		storage_file.read_freelist_trunk()?;

		Ok(storage_file)
	}

	#[inline]
	pub fn page_size(&self) -> usize {
		self.page_size
	}

	#[inline]
	pub fn num_pages(&self) -> u32 {
		let header_buf = self.header_buf.read();
		let header = HeaderPage::from_bytes(&header_buf);
		header.num_pages
	}

	#[inline]
	pub fn read_page(&self, buf: &mut [u8], page_number: NonZeroU32) -> Result<(), StorageError> {
		self.read_page_raw(buf, page_number.get())
	}

	#[inline]
	pub fn write_page(&self, buf: &[u8], page_number: NonZeroU32) -> Result<(), StorageError> {
		self.write_page_raw(buf, page_number.get())
	}

	pub fn allocate_page(&self) -> Result<NonZeroU32, StorageError> {
		if let Some(free_page) = self.pop_free_page()? {
			return Ok(free_page);
		}
		let new_page = self.create_new_page()?;
		Ok(new_page)
	}

	pub fn free_page(&self, page_number: NonZeroU32) -> Result<(), StorageError> {
		let mut cache = self.freelist_cache.lock();
		if let Some(page_num) = cache.page_num {
			let trunk_page = cache.get_page();
			if trunk_page.header.length < trunk_page.items.len() as u32 {
				trunk_page.items[trunk_page.header.length as usize] = Some(page_number);
				trunk_page.header.length += 1;

				self.write_page(&cache.buf, page_num)?;
				return Ok(());
			}
		}

		let next = cache.page_num;
		cache.page_num = Some(page_number);
		let new_trunk_page = cache.get_page();
		new_trunk_page.header = FreelistPageHeader { next, length: 0 };
		new_trunk_page.items.fill(None);

		self.write_page(&cache.buf, page_number)?;
		Ok(())
	}

	fn create_new_page(&self) -> Result<NonZeroU32, StorageError> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		let Some(new_page) = NonZeroU32::new(header.num_pages) else {
			return Err(StorageError::Corrupted);
		};
		header.num_pages += 1;
		self.write_page_raw(&header_buf, 0)?;
		Ok(new_page)
	}

	fn pop_free_page(&self) -> Result<Option<NonZeroU32>, StorageError> {
		let mut cache = self.freelist_cache.lock();
		let Some(page_num) = cache.page_num else {
			return Ok(None);
		};
		let trunk_page = cache.get_page();

		if trunk_page.header.length == 0 {
			self.set_freelist_trunk(trunk_page.header.next)?;
			return Ok(Some(page_num));
		}

		trunk_page.header.length -= 1;
		let Some(popped_page) = trunk_page.items[trunk_page.header.length as usize].take() else {
			return Err(StorageError::Corrupted);
		};

		self.write_page(&cache.buf, page_num)?;
		Ok(Some(popped_page))
	}

	fn read_freelist_trunk(&self) -> Result<(), StorageError> {
		let mut freelist_cache = self.freelist_cache.lock();

		let Some(trunk) = self.freelist_trunk() else {
			return Ok(());
		};
		self.read_page(&mut freelist_cache.buf, trunk)?;
		freelist_cache.page_num = Some(trunk);
		Ok(())
	}

	fn freelist_trunk(&self) -> Option<NonZeroU32> {
		let header_buf = self.header_buf.read();
		let header = HeaderPage::from_bytes(&header_buf);
		header.freelist_trunk
	}

	fn set_freelist_trunk(&self, trunk: Option<NonZeroU32>) -> Result<(), StorageError> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		header.freelist_trunk = trunk;
		self.write_page_raw(&header_buf, 0)
	}

	fn read_page_raw(&self, buf: &mut [u8], page_number: u32) -> Result<(), StorageError> {
		self.locker.lock_shared(page_number);
		let bytes_read;
		unsafe {
			bytes_read = (*self.target.get())
				.read_at(&mut buf[0..self.page_size()], self.offset_of(page_number))?;
			self.locker.unlock_shared(page_number);
		};
		if bytes_read != self.page_size() {
			return Err(StorageError::IncompleteRead);
		}
		Ok(())
	}

	fn write_page_raw(&self, buf: &[u8], page_number: u32) -> Result<(), StorageError> {
		self.locker.lock_exclusive(page_number);
		let bytes_written;
		unsafe {
			bytes_written = (*self.target.get())
				.write_at(&buf[0..self.page_size()], self.offset_of(page_number))?;
			self.locker.unlock_exclusive(page_number);
		};
		if bytes_written != self.page_size() {
			return Err(StorageError::IncompleteWrite);
		}
		Ok(())
	}

	fn offset_of(&self, page_number: u32) -> u64 {
		page_number as u64 * self.page_size() as u64
	}

	fn validate_header(header: &HeaderPage) -> Result<(), StorageError> {
		if header.magic != MAGIC {
			return Err(StorageError::NotAStorageFile);
		}

		if header.format_version != FORMAT_VERSION {
			return Err(StorageError::UnsupportedVersion(header.format_version));
		}

		let Some(byte_order) = ByteOrder::from_byte(header.byte_order) else {
			return Err(StorageError::Corrupted);
		};
		if byte_order != ByteOrder::NATIVE {
			return Err(StorageError::ByteOrderMismatch(byte_order));
		}
		Ok(())
	}
}

struct FreelistCache {
	page_num: Option<NonZeroU32>,
	buf: Box<[u8]>,
}

impl FreelistCache {
	fn new(page_size: usize) -> Self {
		Self {
			page_num: None,
			buf: iter::repeat(0).take(page_size).collect(),
		}
	}

	#[inline]
	fn get_page(&mut self) -> &mut FreelistPage {
		FreelistPage::from_bytes_mut(&mut self.buf)
	}
}
