use std::{
	alloc::{alloc_zeroed, dealloc, Layout},
	collections::HashMap,
	marker::PhantomData,
	mem,
	num::NonZeroU64,
	ptr,
	sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use parking_lot::{
	lock_api::{RawRwLock as _, RawRwLockDowngrade},
	Mutex, RawRwLock, RwLock, RwLockReadGuard,
};
use static_assertions::assert_impl_all;

#[cfg(test)]
use mockall::{automock, concretize};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{
	consts::DEFAULT_PAGE_CACHE_SIZE,
	files::{segment::PAGE_BODY_SIZE, WalIndex},
	utils::cache::CacheReplacer,
};

use super::{physical::WriteOp, PageId, StorageError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageCacheConfig {
	pub page_cache_size: usize,
}

impl Default for PageCacheConfig {
	fn default() -> Self {
		Self {
			page_cache_size: DEFAULT_PAGE_CACHE_SIZE,
		}
	}
}

#[derive(Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C, packed)]
struct BufferedPageHeader {
	wal_generation: u64,
	wal_offset: u64,
	dirty: u8,
}

impl BufferedPageHeader {
	fn new(wal_index: WalIndex, dirty: bool) -> Self {
		Self {
			wal_generation: wal_index.generation,
			wal_offset: wal_index.offset.get(),
			dirty: dirty as u8,
		}
	}

	fn wal_index(&self) -> WalIndex {
		WalIndex::new(
			self.wal_generation,
			NonZeroU64::new(self.wal_offset).expect("Buffered page header corrupted!"),
		)
	}

	fn set_wal_index(&mut self, index: WalIndex) {
		self.wal_offset = index.offset.get();
		self.wal_generation = index.generation;
	}

	fn dirty(&self) -> bool {
		self.dirty != 0
	}

	fn set_dirty(&mut self, dirty: bool) {
		self.dirty = dirty as u8
	}
}

const HEADER_SIZE: usize = mem::size_of::<BufferedPageHeader>();
const BUFFERED_PAGE_SIZE: usize = PAGE_BODY_SIZE + HEADER_SIZE;

struct PageBuffer {
	buf: *mut u8,
	num_pages: usize,
	num_filled: AtomicUsize,
}

impl PageBuffer {
	fn new(num_pages: usize) -> Self {
		let buf_size = num_pages * BUFFERED_PAGE_SIZE;
		let buf = if buf_size != 0 {
			// Safety: buf_size is guaranteed not to be zero, so the layout is not
			// zero-sized.
			unsafe { alloc_zeroed(Layout::from_size_align(buf_size, 1).unwrap()) }
		} else {
			ptr::null_mut()
		};
		Self {
			buf,
			num_pages,
			num_filled: AtomicUsize::new(0),
		}
	}

	fn push_page(&self) -> Option<usize> {
		let num_filled = self.num_filled.load(Ordering::Acquire);
		if num_filled == self.num_pages {
			return None;
		}
		self.num_filled.store(num_filled + 1, Ordering::Release);
		Some(num_filled)
	}

	fn page_ptr(&self, index: usize) -> Option<*mut u8> {
		if index >= self.num_pages {
			return None;
		}
		// Safety: the resulting pointer is guaranteed to be in the allocated buffer.
		Some(unsafe { self.buf.add(index * BUFFERED_PAGE_SIZE) })
	}

	/// # Safety:
	/// The caller must ensure that no mutable reference to the same page
	/// exists.
	unsafe fn get_page(&self, index: usize) -> Option<&[u8]> {
		Some(std::slice::from_raw_parts(
			self.page_ptr(index)?,
			BUFFERED_PAGE_SIZE,
		))
	}

	/// # Safety:
	/// The caller must ensure that no shared reference, and no other mutable
	/// references to the same page exist.
	unsafe fn get_page_mut(&self, index: usize) -> Option<&mut [u8]> {
		Some(std::slice::from_raw_parts_mut(
			self.page_ptr(index)?,
			BUFFERED_PAGE_SIZE,
		))
	}
}

impl Drop for PageBuffer {
	fn drop(&mut self) {
		if !self.buf.is_null() {
			let buf_size = self.num_pages * BUFFERED_PAGE_SIZE;
			// Safety:
			// - `self.buf` is guaranteed not to be null
			// - The buffer is never reallocated, so the layout stays the same
			unsafe {
				dealloc(
					self.buf,
					Layout::from_size_align(buf_size, mem::align_of::<BufferedPageHeader>())
						.unwrap(),
				)
			}
		}
	}
}

#[derive(Clone)]
pub(crate) struct PageReadGuard<'a> {
	page: &'a [u8],
	lock: &'a RawRwLock,
	_marker: PhantomData<RwLockReadGuard<'a, [u8]>>,
}

#[cfg_attr(test, automock)]
pub(crate) trait PageReadGuardApi {
	fn header(&self) -> &BufferedPageHeader;
	fn body(&self) -> &[u8];
	fn read(&self, offset: usize, buf: &mut [u8]);
}

impl<'a> PageReadGuardApi for PageReadGuard<'a> {
	fn header(&self) -> &BufferedPageHeader {
		BufferedPageHeader::ref_from(&self.page[0..HEADER_SIZE]).unwrap()
	}

	fn body(&self) -> &[u8] {
		&self.page[HEADER_SIZE..]
	}

	fn read(&self, offset: usize, buf: &mut [u8]) {
		buf.copy_from_slice(&self.body()[offset..offset + buf.len()]);
	}
}

impl<'a> Drop for PageReadGuard<'a> {
	fn drop(&mut self) {
		// Safety: the existence of this object guarantees the lock is owned by the
		// current context
		unsafe { self.lock.unlock_shared() };
	}
}

pub(crate) struct PageWriteGuard<'a> {
	index: usize,
	page: &'a mut [u8],
	lock: &'a RawRwLock,
	_marker: PhantomData<RwLockReadGuard<'a, [u8]>>,
}

impl<'a> PageWriteGuard<'a> {
	fn body_mut(&mut self) -> &mut [u8] {
		&mut self.page[HEADER_SIZE..]
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait PageWriteGuardApi {
	fn header(&self) -> &BufferedPageHeader;
	fn body(&self) -> &[u8];
	fn header_mut(&mut self) -> &mut BufferedPageHeader;
	fn read(&self, offset: usize, buf: &mut [u8]);
	fn write(&mut self, offset: usize, buf: &[u8], wal_index: WalIndex);
}

impl<'a> PageWriteGuardApi for PageWriteGuard<'a> {
	fn header(&self) -> &BufferedPageHeader {
		BufferedPageHeader::ref_from(&self.page[0..HEADER_SIZE]).unwrap()
	}

	fn header_mut(&mut self) -> &mut BufferedPageHeader {
		BufferedPageHeader::mut_from(&mut self.page[0..HEADER_SIZE]).unwrap()
	}

	fn body(&self) -> &[u8] {
		&self.page[HEADER_SIZE..]
	}

	fn read(&self, offset: usize, buf: &mut [u8]) {
		buf.copy_from_slice(&self.body()[offset..offset + buf.len()]);
	}

	fn write(&mut self, offset: usize, buf: &[u8], wal_index: WalIndex) {
		self.header_mut().set_wal_index(wal_index);
		self.body_mut()[offset..offset + buf.len()].copy_from_slice(buf);
	}
}

impl<'a> Drop for PageWriteGuard<'a> {
	fn drop(&mut self) {
		// Safety: the existence of this object guarantees the lock is owned by the
		// current context
		unsafe { self.lock.unlock_exclusive() };
	}
}

pub(crate) struct PageCache {
	buf: PageBuffer,
	indices: RwLock<HashMap<PageId, usize>>,
	replacer: RwLock<CacheReplacer<PageId>>,
	scrap: Mutex<Vec<usize>>,
	has_scrap: AtomicBool,
	dirty_list: Mutex<Vec<PageId>>,
	locks: Box<[RawRwLock]>,
}
assert_impl_all!(PageCache: Send, Sync);

// Safety: `buf`'s internal pointer is never leaked in any form.
unsafe impl Send for PageCache {}

// Safety: `buf` is only accessed through the `load` and `store` methods,
// which guarantee the safety of the references by acquiring the corresponding
// locks.
unsafe impl Sync for PageCache {}

impl PageCache {
	pub fn new(config: &PageCacheConfig) -> Self {
		let num_pages = config.page_cache_size / BUFFERED_PAGE_SIZE;
		let buf = PageBuffer::new(num_pages);
		let replacer = CacheReplacer::new(num_pages);
		Self {
			buf,
			replacer: RwLock::new(replacer),
			indices: RwLock::new(HashMap::new()),
			scrap: Mutex::new(Vec::new()),
			has_scrap: AtomicBool::new(false),
			dirty_list: Mutex::new(Vec::new()),
			locks: std::iter::repeat_with(|| RawRwLock::INIT)
				.take(num_pages)
				.collect(),
		}
	}

	fn evict_for(&self, page_id: PageId) -> Option<PageId> {
		let mut replacer = self.replacer.write();
		let mut maybe_evict = replacer.evict_replace(page_id);
		mem::drop(replacer);

		loop {
			if let Some(evicted) = maybe_evict {
				let indices = self.indices.read();
				let index = *indices
					.get(&evicted)
					.expect("Tried to evict a page that is not in the cache!");

				// If we are trying to evict the same page that we're inserting, or if the page
				// we're trying to evict is currently locked, we reinsert it and try the next
				// candidate.
				//
				// Note that this ends up in an infinite loop if all pages in the cache are
				// locked over an extended period, but that should rarely happen.
				if evicted == page_id || self.locks[index].is_locked() {
					let mut replacer = self.replacer.write();
					maybe_evict = replacer.evict_replace(evicted);
					continue;
				}
			}
			break;
		}
		maybe_evict
	}

	fn get_store_index(&self, page_id: PageId) -> usize {
		let indices = self.indices.read();
		if let Some(stored_index) = indices.get(&page_id).copied() {
			return stored_index;
		}
		mem::drop(indices);

		if self.has_scrap.load(Ordering::Relaxed) {
			let mut scrap = self.scrap.lock();
			if let Some(scrap_index) = scrap.pop() {
				return scrap_index;
			}
		}

		let maybe_evict = self.evict_for(page_id);
		let mut indices = self.indices.write();
		if let Some(evict) = maybe_evict {
			let index = indices
				.remove(&evict)
				.expect("Tried to evict a page that is not in the cache!");
			indices.insert(page_id, index);
			index
		} else {
			let index = self
				.buf
				.push_page()
				.expect("Failed to evict a page when the buffer was full!");
			indices.insert(page_id, index);
			index
		}
	}

	fn load_direct(&self, index: usize) -> PageReadGuard<'_> {
		let lock = &self.locks[index];
		lock.lock_shared();
		// Safety: The safety of the reference is guaranteed by acquiring the shared
		// lock.
		let page =
			unsafe { self.buf.get_page(index) }.expect("Tried to index page buffer out of bounds!");

		PageReadGuard {
			lock,
			page,
			_marker: PhantomData,
		}
	}

	fn store_direct(&self, index: usize) -> PageWriteGuard<'_> {
		let lock = &self.locks[index];
		lock.lock_exclusive();
		// Safety: The safety of the reference is guaranteed by acquiring the exclusive
		// lock.
		let page = unsafe { self.buf.get_page_mut(index) }
			.expect("Triet to index page buffer out of bounds!");

		PageWriteGuard {
			index,
			lock,
			page,
			_marker: PhantomData,
		}
	}
}

#[cfg_attr(test, automock(
    type ReadGuard<'a> = MockPageReadGuardApi;
    type WriteGuard<'a> = MockPageWriteGuardApi;
))]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait PageCacheApi {
	type ReadGuard<'a>: PageReadGuardApi + 'a
	where
		Self: 'a;
	type WriteGuard<'a>: PageWriteGuardApi + 'a
	where
		Self: 'a;

	fn has_page(&self, page_id: PageId) -> bool;
	fn load<'a>(&'a self, page_id: PageId) -> Option<Self::ReadGuard<'a>>;
	fn store<'a>(&'a self, page_id: PageId) -> Self::WriteGuard<'a>;
	fn scrap(&self, page_id: PageId);
	fn downgrade_guard<'a>(&'a self, guard: Self::WriteGuard<'a>) -> Self::ReadGuard<'a>;

	#[cfg_attr(test, concretize)]
	fn flush<HFn>(&self, handler: HFn) -> Result<(), StorageError>
	where
		HFn: FnMut(WriteOp) -> Result<(), StorageError>;
}

impl PageCacheApi for PageCache {
	type ReadGuard<'a> = PageReadGuard<'a>;
	type WriteGuard<'a> = PageWriteGuard<'a>;

	fn has_page(&self, page_id: PageId) -> bool {
		let indices = self.indices.read();
		indices.contains_key(&page_id)
	}

	fn load(&self, page_id: PageId) -> Option<PageReadGuard<'_>> {
		let indices = self.indices.read();
		let index = indices.get(&page_id).copied()?;
		mem::drop(indices);

		let replacer = self.replacer.read();
		let access_successful = replacer.access(&page_id);
		debug_assert!(access_successful);
		mem::drop(replacer);

		Some(self.load_direct(index))
	}

	fn store(&self, page_id: PageId) -> PageWriteGuard<'_> {
		let mut dirty_list = self.dirty_list.lock();
		dirty_list.push(page_id);
		mem::drop(dirty_list);

		let index = self.get_store_index(page_id);
		self.store_direct(index)
	}

	fn scrap(&self, page_id: PageId) {
		let mut indices = self.indices.write();
		let Some(index) = indices.remove(&page_id) else {
			return;
		};
		mem::drop(indices);

		self.has_scrap.store(true, Ordering::Relaxed);
		self.scrap.lock().push(index);
	}

	fn downgrade_guard<'a>(&'a self, guard: PageWriteGuard<'a>) -> PageReadGuard<'a> {
		let lock = guard.lock;
		// Safety: the existance of the PageWriteGuard guarantees that the lock is owned
		// in the current context
		unsafe { lock.downgrade() };

		// Safety: we have the shared lock for this page
		let page = unsafe { self.buf.get_page(guard.index) }
			.expect("Got out of bounds buffer index while upgrading guard");

		mem::forget(guard);
		PageReadGuard {
			page,
			lock,
			_marker: PhantomData,
		}
	}

	fn flush<HFn>(&self, mut handler: HFn) -> Result<(), StorageError>
	where
		HFn: FnMut(WriteOp) -> Result<(), StorageError>,
	{
		let mut dirty_list_guard = self.dirty_list.lock();
		let dirty_list = dirty_list_guard.clone();
		dirty_list_guard.clear();
		mem::drop(dirty_list_guard);

		let mut error: Option<StorageError> = None;
		for page_id in dirty_list.iter().copied() {
			let indices = self.indices.read();
			let Some(index) = indices.get(&page_id).copied() else {
				continue;
			};
			let guard = self.load_direct(index);
			if !guard.header().dirty() {
				continue;
			}
			if let Err(err) = handler(WriteOp {
				wal_index: guard.header().wal_index(),
				page_id,
				buf: guard.page,
			}) {
				error = Some(err);
				break;
			};
		}

		if let Some(err) = error {
			let mut dirty_list_guard = self.dirty_list.lock();
			dirty_list_guard.extend(&dirty_list);
			return Err(err);
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use pretty_assertions::assert_buf_eq;

	use crate::{
		storage::test_helpers::{page_id, wal_index},
		utils::units::MIB,
	};

	use super::*;

	#[test]
	fn load_and_store() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let expected_page = [69; PAGE_BODY_SIZE];
		cache
			.store(page_id!(69, 420))
			.write(0, &expected_page, wal_index!(1, 2));

		let mut received_page = [0; PAGE_BODY_SIZE];
		cache
			.load(page_id!(69, 420))
			.unwrap()
			.read(0, &mut received_page);

		// then
		assert_buf_eq!(expected_page, received_page);
	}

	#[test]
	fn load_cache_miss() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let guard = cache.load(page_id!(69, 420));

		// then
		assert!(guard.is_none())
	}

	#[test]
	fn evict_correct_page() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 4 * PAGE_BODY_SIZE,
		});

		// when
		cache.store(page_id!(1, 1)); // add 1, 1 to recent
		cache.store(page_id!(2, 2)); // add 2, 2 to recent
		cache.store(page_id!(3, 3)); // add 3, 3 to recent
		cache.store(page_id!(4, 4)); // add 4, 4 to recent
		cache.load(page_id!(1, 1)); // 1, 1 was referenced in recent
		cache.load(page_id!(2, 2)); // 2, 2 was referenced in recent
		cache.load(page_id!(1, 1)); // 1, 1 is promoted to frequent

		// recent is large, therefore 3, 3 is evicted as it is the first
		// non-referenced item in frequent
		cache.store(page_id!(5, 5));

		// then
		assert!(cache.load(page_id!(1, 1)).is_some());
		assert!(cache.load(page_id!(2, 2)).is_some());
		assert!(cache.load(page_id!(3, 3)).is_none());
		assert!(cache.load(page_id!(4, 4)).is_some());
		assert!(cache.load(page_id!(5, 5)).is_some());
	}

	#[test]
	fn doesnt_evict_locked_page() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 4 * PAGE_BODY_SIZE,
		});

		// when
		cache.store(page_id!(1, 1)); // add 1, 1 to recent
		cache.store(page_id!(2, 2)); // add 2, 2 to recent
		let guard = cache.store(page_id!(3, 3)); // add 3, 3 to recent
		cache.store(page_id!(4, 4)); // add 4, 4 to recent
		cache.load(page_id!(1, 1)); // 1, 1 was referenced in recent
		cache.load(page_id!(2, 2)); // 2, 2 was referenced in recent
		cache.load(page_id!(1, 1)); // 1, 1 is promoted to frequent

		// recent is large, therefore 3, 3 would be evicted, but it is locked, so 4, 4
		// is evicted instead
		cache.store(page_id!(5, 5));

		mem::drop(guard);

		// then
		assert!(cache.load(page_id!(1, 1)).is_some());
		assert!(cache.load(page_id!(2, 2)).is_some());
		assert!(cache.load(page_id!(3, 3)).is_some());
		assert!(cache.load(page_id!(4, 4)).is_none());
		assert!(cache.load(page_id!(5, 5)).is_some());
	}
}
