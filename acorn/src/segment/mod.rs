use parking_lot::{Mutex, RwLock};
use std::{cell::UnsafeCell, io, iter, mem::size_of, num::NonZeroU16, usize};
use thiserror::Error;

use crate::consts::DEFAULT_PAGE_SIZE;
use crate::{
	consts::{validate_page_size, PageSizeBoundsError, SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC},
	segment::format::{FreelistPageHeader, HeaderPage},
	utils::{byte_order::ByteOrder, byte_view::ByteView},
};

mod format;
mod lock;
mod target;

pub use format::*;
pub use target::*;

use self::lock::PageLocker;

pub type PageNumber = NonZeroU16;

#[derive(Debug, Error)]
pub enum Error {
	#[error("The provided file is not an acorn segment file (expected magic bytes {SEGMENT_MAGIC:08x?})")]
	NotASegmentFile,

	#[error("Segment format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("Cannot open a {0} segment file on a {} system", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error("The segment is corrupted (Unexpected end of file)")]
	IncompleteRead,

	#[error("Failed to expand segment file")]
	IncompleteWrite,

	#[error("The segment file is corrupted")]
	Corrupted,

	#[error("An error occurred accessing the segment file: {0}")]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Failed to write the complete file header")]
	IncompleteWrite,

	#[error(transparent)]
	PageSizeBounds(#[from] PageSizeBoundsError),

	#[error("An error occurred initializing the storage file: {0}")]
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

pub struct Segment<T: IoTarget> {
	header_buf: RwLock<Box<[u8]>>,
	freelist_cache: Mutex<FreelistCache>,
	page_size: u16,
	target: UnsafeCell<T>,
	locker: PageLocker,
}

impl<T: IoTarget> Segment<T> {
	pub fn init(target: &mut T, params: InitParams) -> Result<(), InitError> {
		validate_page_size(params.page_size)?;

		let mut header_buf: [u8; size_of::<HeaderPage>()] = Default::default();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		*header = HeaderPage {
			magic: SEGMENT_MAGIC,
			format_version: SEGMENT_FORMAT_VERSION,
			page_size: params.page_size,
			byte_order: ByteOrder::NATIVE as u8,
			num_pages: 1,
			freelist_trunk: None,
		};
		if target.write_at(&header_buf, 0)? != header_buf.len() {
			return Err(InitError::IncompleteWrite);
		}
		Ok(())
	}

	pub fn load(target: T) -> Result<Self, Error> {
		let mut buf: [u8; size_of::<HeaderPage>()] = Default::default();
		let bytes_read = target.read_at(&mut buf, 0)?;
		if bytes_read != buf.len() {
			return Err(Error::IncompleteRead);
		}
		let header = HeaderPage::from_bytes(&buf);
		Self::validate_header(header)?;

		let mut header_buf: Box<[u8]> = iter::repeat(0).take(header.page_size as usize).collect();
		header_buf[0..size_of::<HeaderPage>()].copy_from_slice(&buf);

		let storage_file = Self {
			header_buf: RwLock::new(header_buf),
			freelist_cache: Mutex::new(FreelistCache::new(header.page_size as usize)),
			page_size: header.page_size,
			target: UnsafeCell::new(target),
			locker: PageLocker::new(),
		};
		storage_file.read_freelist_trunk()?;

		Ok(storage_file)
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.page_size
	}

	#[inline]
	pub fn num_pages(&self) -> u16 {
		let header_buf = self.header_buf.read();
		let header = HeaderPage::from_bytes(&header_buf);
		header.num_pages
	}

	#[inline]
	pub fn read_page(&self, buf: &mut [u8], page_number: PageNumber) -> Result<(), Error> {
		self.read_page_raw(buf, page_number.get())
	}

	#[inline]
	pub fn write_page(&self, buf: &[u8], page_number: PageNumber) -> Result<(), Error> {
		self.write_page_raw(buf, page_number.get())
	}

	pub fn allocate_page(&self) -> Result<PageNumber, Error> {
		if let Some(free_page) = self.pop_free_page()? {
			return Ok(free_page);
		}
		let new_page = self.create_new_page()?;
		Ok(new_page)
	}

	pub fn free_page(&self, page_number: PageNumber) -> Result<(), Error> {
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

	fn create_new_page(&self) -> Result<PageNumber, Error> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		let Some(new_page) = PageNumber::new(header.num_pages) else {
			return Err(Error::Corrupted);
		};
		header.num_pages += 1;
		self.write_page_raw(&header_buf, 0)?;

		let zeroed_buf: Box<[u8]> = iter::repeat(0).take(self.page_size.into()).collect();
		self.write_page(&zeroed_buf, new_page)?;
		Ok(new_page)
	}

	fn pop_free_page(&self) -> Result<Option<PageNumber>, Error> {
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
			return Err(Error::Corrupted);
		};

		self.write_page(&cache.buf, page_num)?;
		Ok(Some(popped_page))
	}

	fn read_freelist_trunk(&self) -> Result<(), Error> {
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

	fn set_freelist_trunk(&self, trunk: Option<PageNumber>) -> Result<(), Error> {
		let mut header_buf = self.header_buf.write();
		let header = HeaderPage::from_bytes_mut(&mut header_buf);
		header.freelist_trunk = trunk;
		self.write_page_raw(&header_buf, 0)
	}

	fn read_page_raw(&self, buf: &mut [u8], page_number: u16) -> Result<(), Error> {
		self.locker.lock_shared(page_number);
		let bytes_read;
		unsafe {
			bytes_read = (*self.target.get()).read_at(
				&mut buf[0..self.page_size().into()],
				self.offset_of(page_number),
			)?;
			self.locker.unlock_shared(page_number);
		};
		if bytes_read != self.page_size().into() {
			return Err(Error::IncompleteRead);
		}
		Ok(())
	}

	fn write_page_raw(&self, buf: &[u8], page_number: u16) -> Result<(), Error> {
		self.locker.lock_exclusive(page_number);
		let bytes_written;
		unsafe {
			bytes_written = (*self.target.get()).write_at(
				&buf[0..self.page_size().into()],
				self.offset_of(page_number),
			)?;
			self.locker.unlock_exclusive(page_number);
		};
		if bytes_written != self.page_size().into() {
			return Err(Error::IncompleteWrite);
		}
		Ok(())
	}

	fn offset_of(&self, page_number: u16) -> u64 {
		page_number as u64 * self.page_size() as u64
	}

	fn validate_header(header: &HeaderPage) -> Result<(), Error> {
		if header.magic != SEGMENT_MAGIC {
			return Err(Error::NotASegmentFile);
		}

		if header.format_version != SEGMENT_FORMAT_VERSION {
			return Err(Error::UnsupportedVersion(header.format_version));
		}

		let Some(byte_order) = ByteOrder::from_byte(header.byte_order) else {
			return Err(Error::Corrupted);
		};
		if byte_order != ByteOrder::NATIVE {
			return Err(Error::ByteOrderMismatch(byte_order));
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

	use crate::utils::units::{KiB, B};

	use super::*;

	#[test]
	fn init_file() {
		let mut file = Vec::new();

		Segment::init(
			&mut file,
			InitParams {
				page_size: 32 * KiB as u16,
			},
		)
		.unwrap();

		let header = HeaderPage::from_bytes(&file);
		assert_eq!(header.magic, *b"ACNS");
		assert_eq!(header.format_version, 1);
		assert_eq!(header.byte_order, ByteOrder::NATIVE as u8);
		assert_eq!(header.page_size, 32 * KiB as u16);
		assert_eq!(header.num_pages, 1);
		assert_eq!(header.freelist_trunk, None);
	}

	#[test]
	fn try_init_with_non_power_of_two_page_size() {
		let mut file = Vec::new();
		let result = Segment::init(
			&mut file,
			InitParams {
				page_size: 31 * KiB as u16,
			},
		);
		assert_matches!(result, Err(InitError::PageSizeBounds(..)));
	}

	#[test]
	fn try_init_with_too_small_page_size() {
		let mut file = Vec::new();
		let result = Segment::init(
			&mut file,
			InitParams {
				page_size: 256 * B as u16,
			},
		);
		assert_matches!(result, Err(InitError::PageSizeBounds(..)));
	}

	#[test]
	fn load_file() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACNS";
		header.format_version = 1;
		header.byte_order = ByteOrder::NATIVE as u8;
		header.page_size = 16 * KiB as u16;
		header.num_pages = 1;
		header.freelist_trunk = None;

		let storage = Segment::load(file).unwrap();
		assert_eq!(storage.page_size(), 16 * KiB as u16);
	}

	#[test]
	fn try_load_without_magic() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"AAAA";

		match Segment::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, Error::NotASegmentFile),
		}
	}

	#[test]
	fn try_load_with_wrong_format_version() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACNS";
		header.format_version = 69;

		match Segment::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, Error::UnsupportedVersion(..)),
		}
	}

	#[test]
	fn try_load_with_wrong_byte_order() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACNS";
		header.format_version = 1;
		header.byte_order = match ByteOrder::NATIVE {
			ByteOrder::Big => ByteOrder::Little as u8,
			ByteOrder::Little => ByteOrder::Big as u8,
		};

		match Segment::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, Error::ByteOrderMismatch(..)),
		}
	}

	#[test]
	fn try_load_incomplete_file() {
		let file: Vec<u8> = iter::repeat(0).take(10 * B).collect();

		match Segment::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, Error::IncompleteRead),
		}
	}

	#[test]
	fn try_load_with_corrupted_byte_order() {
		let mut file: Vec<u8> = iter::repeat(0).take(16 * KiB).collect();
		let header = HeaderPage::from_bytes_mut(&mut file);
		header.magic = *b"ACNS";
		header.format_version = 1;
		header.byte_order = 2;

		match Segment::load(file) {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, Error::Corrupted),
		}
	}

	#[test]
	fn simple_alloc_write_read() {
		let mut file: Vec<u8> = Vec::new();
		Segment::init(&mut file, InitParams::default()).unwrap();
		let storage = Segment::load(file).unwrap();

		let mut src_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();
		let mut dst_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();

		let page_num = storage.allocate_page().unwrap();

		src_buf.fill(69);
		src_buf[0] = 25;
		src_buf[storage.page_size() as usize - 1] = 42;
		storage.write_page(&src_buf, page_num).unwrap();

		storage.read_page(&mut dst_buf, page_num).unwrap();

		assert_eq!(src_buf, dst_buf);
	}

	#[test]
	fn simple_free() {
		let mut file: Vec<u8> = Vec::new();
		Segment::init(&mut file, InitParams::default()).unwrap();
		let storage = Segment::load(file).unwrap();

		let mut src_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();
		let mut dst_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();

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
		Segment::init(&mut file, InitParams { page_size: 512 }).unwrap();
		let storage = Segment::load(file).unwrap();

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
		Segment::init(&mut file, InitParams::default()).unwrap();
		let storage = Segment::load(file).unwrap();

		let allocated = storage.allocate_page().unwrap();

		let mut buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();
		storage.read_page(&mut buf, allocated).unwrap()
	}
}
