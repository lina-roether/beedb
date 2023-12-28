use parking_lot::{Mutex, RwLock};
use std::{cell::UnsafeCell, io, iter, mem::size_of, num::NonZeroU16, usize};
use thiserror::Error;

use crate::{
	storage::format::{FreelistPageHeader, HeaderPage},
	utils::{
		byte_order::ByteOrder,
		byte_view::ByteView,
		units::{display_size, KiB, B},
	},
};

mod format;
mod lock;
mod target;

pub use format::*;
pub use target::*;

use self::lock::PageLocker;

pub type PageNumber = NonZeroU16;

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

	#[error(
		"Page size {0} is invalid; must be a power of two and at between {} and {}",
		display_size(MIN_PAGE_SIZE),
		display_size(MAX_PAGE_SIZE)
	)]
	InvalidPageSize(usize),

	#[error("An error occurred initializing the storage file: {0}")]
	Io(#[from] io::Error),
}

pub const MAGIC: [u8; 4] = *b"ACRN";
pub const FORMAT_VERSION: u8 = 1;
pub const MIN_PAGE_SIZE: usize = 512 * B;
pub const DEFAULT_PAGE_SIZE: usize = 16 * KiB;
pub const MAX_PAGE_SIZE: usize = 64 * KiB;

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

pub struct Storage<T: IoTarget> {
	header_buf: RwLock<Box<[u8]>>,
	freelist_cache: Mutex<FreelistCache>,
	page_size: usize,
	target: UnsafeCell<T>,
	locker: PageLocker,
}

impl<T: IoTarget> Storage<T> {
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
			num_pages: 1,
			freelist_trunk: None,
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
	pub fn num_pages(&self) -> u16 {
		let header_buf = self.header_buf.read();
		let header = HeaderPage::from_bytes(&header_buf);
		header.num_pages
	}

	#[inline]
	pub fn read_page(&self, buf: &mut [u8], page_number: PageNumber) -> Result<(), StorageError> {
		self.read_page_raw(buf, page_number.get())
	}

	#[inline]
	pub fn write_page(&self, buf: &[u8], page_number: PageNumber) -> Result<(), StorageError> {
		self.write_page_raw(buf, page_number.get())
	}

	pub fn allocate_page(&self) -> Result<PageNumber, StorageError> {
		if let Some(free_page) = self.pop_free_page()? {
			return Ok(free_page);
		}
		let new_page = self.create_new_page()?;
		Ok(new_page)
	}

	pub fn free_page(&self, page_number: PageNumber) -> Result<(), StorageError> {
		let mut cache = self.freelist_cache.lock();
		if let Some(page_num) = cache.page_num {
			let trunk_page = cache.get_page();
			if trunk_page.header.length < trunk_page.items.len() as u16 {
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

	fn create_new_page(&self) -> Result<PageNumber, StorageError> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		let Some(new_page) = PageNumber::new(header.num_pages) else {
			return Err(StorageError::Corrupted);
		};
		header.num_pages += 1;
		self.write_page_raw(&header_buf, 0)?;

		let zeroed_buf: Box<[u8]> = iter::repeat(0).take(self.page_size).collect();
		self.write_page(&zeroed_buf, new_page)?;
		Ok(new_page)
	}

	fn pop_free_page(&self) -> Result<Option<PageNumber>, StorageError> {
		let mut cache = self.freelist_cache.lock();
		let Some(page_num) = cache.page_num else {
			return Ok(None);
		};
		let trunk_page = cache.get_page();

		if trunk_page.header.length == 0 {
			let new_trunk = trunk_page.header.next;
			self.set_freelist_trunk(new_trunk)?;
			cache.page_num = new_trunk;
			if let Some(new_trunk) = new_trunk {
				self.read_page(&mut cache.buf, new_trunk)?;
			}
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

	fn freelist_trunk(&self) -> Option<PageNumber> {
		let header_buf = self.header_buf.read();
		let header = HeaderPage::from_bytes(&header_buf);
		header.freelist_trunk
	}

	fn set_freelist_trunk(&self, trunk: Option<PageNumber>) -> Result<(), StorageError> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		header.freelist_trunk = trunk;
		self.write_page_raw(&header_buf, 0)
	}

	fn read_page_raw(&self, buf: &mut [u8], page_number: u16) -> Result<(), StorageError> {
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

	fn write_page_raw(&self, buf: &[u8], page_number: u16) -> Result<(), StorageError> {
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

	fn offset_of(&self, page_number: u16) -> u64 {
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
	page_num: Option<PageNumber>,
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

#[cfg(test)]
mod tests {
	use std::{assert_matches::assert_matches, collections::HashSet, hash::Hash};

	use super::*;

	#[test]
	fn init_file() {
		let mut file = Vec::new();

		Storage::init(
			&mut file,
			InitParams {
				page_size: 32 * KiB,
			},
		)
		.unwrap();

		let header = HeaderPage::from_bytes(&file);
		assert_eq!(header.magic, *b"ACRN");
		assert_eq!(header.format_version, 1);
		assert_eq!(header.byte_order, ByteOrder::NATIVE as u8);
		assert_eq!(header.page_size_exponent, 15);
		assert_eq!(header.num_pages, 1);
		assert_eq!(header.freelist_trunk, None);
	}

	#[test]
	fn try_init_with_non_power_of_two_page_size() {
		let mut file = Vec::new();
		let result = Storage::init(
			&mut file,
			InitParams {
				page_size: 31 * KiB,
			},
		);
		assert_matches!(result, Err(InitError::InvalidPageSize(..)));
	}

	#[test]
	fn try_init_with_too_small_page_size() {
		let mut file = Vec::new();
		let result = Storage::init(&mut file, InitParams { page_size: 256 * B });
		assert_matches!(result, Err(InitError::InvalidPageSize(..)));
	}

	#[test]
	fn load_file() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACRN";
		header.format_version = 1;
		header.byte_order = ByteOrder::NATIVE as u8;
		header.page_size_exponent = 14;
		header.num_pages = 1;
		header.freelist_trunk = None;

		let storage = Storage::load(file).unwrap();
		assert_eq!(storage.page_size(), 16 * KiB);
	}

	#[test]
	fn try_load_without_magic() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"AAAA";

		match Storage::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, StorageError::NotAStorageFile),
		}
	}

	#[test]
	fn try_load_with_wrong_format_version() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACRN";
		header.format_version = 69;

		match Storage::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, StorageError::UnsupportedVersion(..)),
		}
	}

	#[test]
	fn try_load_with_wrong_byte_order() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACRN";
		header.format_version = 1;
		header.byte_order = match ByteOrder::NATIVE {
			ByteOrder::Big => ByteOrder::Little as u8,
			ByteOrder::Little => ByteOrder::Big as u8,
		};

		match Storage::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, StorageError::ByteOrderMismatch(..)),
		}
	}

	#[test]
	fn try_load_incomplete_file() {
		let file: Vec<u8> = iter::repeat(0).take(10 * B).collect();

		match Storage::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, StorageError::IncompleteRead),
		}
	}

	#[test]
	fn try_load_with_corrupted_byte_order() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACRN";
		header.format_version = 1;
		header.byte_order = 2;

		match Storage::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, StorageError::Corrupted),
		}
	}

	#[test]
	fn simple_alloc_write_read() {
		let mut file: Vec<u8> = Vec::new();
		Storage::init(&mut file, InitParams::default()).unwrap();
		let storage = Storage::load(file).unwrap();

		let mut src_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size()).collect();
		let mut dst_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size()).collect();

		let page_num = storage.allocate_page().unwrap();

		src_buf.fill(69);
		src_buf[0] = 25;
		src_buf[storage.page_size() - 1] = 42;
		storage.write_page(&src_buf, page_num).unwrap();

		storage.read_page(&mut dst_buf, page_num).unwrap();

		assert_eq!(src_buf, dst_buf);
	}

	#[test]
	fn simple_free() {
		let mut file: Vec<u8> = Vec::new();
		Storage::init(&mut file, InitParams::default()).unwrap();
		let storage = Storage::load(file).unwrap();

		let mut src_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size()).collect();
		let mut dst_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size()).collect();

		let page_num_1 = storage.allocate_page().unwrap();
		let page_num_2 = storage.allocate_page().unwrap();

		storage.free_page(page_num_1).unwrap();
		storage.free_page(page_num_2).unwrap();

		let page_num_3 = storage.allocate_page().unwrap();
		let page_num_4 = storage.allocate_page().unwrap();

		src_buf.fill(69);
		storage.write_page(&src_buf, page_num_3).unwrap();

		src_buf.fill(25);
		storage.write_page(&src_buf, page_num_4).unwrap();

		src_buf.fill(69);
		storage.read_page(&mut dst_buf, page_num_3).unwrap();
		assert_eq!(src_buf, dst_buf);

		src_buf.fill(25);
		storage.read_page(&mut dst_buf, page_num_4).unwrap();
		assert_eq!(src_buf, dst_buf);

		assert_eq!(storage.num_pages(), 3);
	}

	#[test]
	fn saturating_alloc_free() {
		let mut file: Vec<u8> = Vec::new();
		Storage::init(&mut file, InitParams { page_size: 512 }).unwrap();
		let storage = Storage::load(file).unwrap();

		let mut pages: Vec<PageNumber> = Vec::new();
		for _ in 0..500 {
			pages.push(storage.allocate_page().unwrap());
		}
		for page in pages.iter().copied() {
			storage.free_page(page).unwrap();
		}
		let mut reallocated_pages: Vec<PageNumber> = Vec::new();
		for _ in 0..500 {
			reallocated_pages.push(storage.allocate_page().unwrap());
		}

		assert!(has_no_duplicates(pages));
		assert!(has_no_duplicates(reallocated_pages));
		assert_eq!(storage.num_pages(), 501);
	}

	fn has_no_duplicates<T: Eq + Hash>(items: impl IntoIterator<Item = T>) -> bool {
		let mut known_values: HashSet<T> = HashSet::new();
		for item in items {
			if known_values.contains(&item) {
				return false;
			}
			known_values.insert(item);
		}
		true
	}

	#[test]
	fn can_read_after_alloc() {
		let mut file = Vec::new();
		Storage::init(&mut file, InitParams::default()).unwrap();
		let storage = Storage::load(file).unwrap();

		let allocated = storage.allocate_page().unwrap();

		let mut buf: Box<[u8]> = iter::repeat(0).take(storage.page_size()).collect();
		storage.read_page(&mut buf, allocated).unwrap()
	}
}
