use std::{
	alloc::{alloc_zeroed, dealloc, Layout},
	collections::HashMap,
	marker::PhantomData,
	mem,
	num::NonZeroU64,
	ptr::{self, NonNull},
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc,
	},
	time::Duration,
};

use futures::executor::ThreadPool;
use log::error;
use parking_lot::{
	lock_api::{RawRwLock as _, RawRwLockDowngrade},
	Mutex, RawRwLock, RwLock, RwLockReadGuard,
};
use static_assertions::assert_impl_all;

#[cfg(test)]
use mockall::automock;

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
	consts::{DEFAULT_FLUSH_PERIOD, DEFAULT_MAX_DIRTY_PAGES, DEFAULT_PAGE_CACHE_SIZE},
	files::{segment::PAGE_BODY_SIZE, WalIndex},
	tasks::{Timer, TimerHandle},
	utils::cache::CacheReplacer,
};

use super::{
	physical::{Op, PhysicalStorage, PhysicalStorageApi, WriteOp},
	PageAddress, StorageError,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PageCacheConfig {
	pub page_cache_size: usize,
	pub max_dirty_pages: f32,
	pub flush_period: Duration,
}

impl Default for PageCacheConfig {
	fn default() -> Self {
		Self {
			page_cache_size: DEFAULT_PAGE_CACHE_SIZE,
			max_dirty_pages: DEFAULT_MAX_DIRTY_PAGES,
			flush_period: DEFAULT_FLUSH_PERIOD,
		}
	}
}

#[derive(Debug, Immutable, KnownLayout, FromBytes, IntoBytes)]
#[repr(C, packed)]
pub(crate) struct BufferedPageHeader {
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

	pub fn wal_index(&self) -> WalIndex {
		WalIndex::new(
			self.wal_generation,
			NonZeroU64::new(self.wal_offset).expect("Buffered page header corrupted!"),
		)
	}

	pub fn set_wal_index(&mut self, index: WalIndex) {
		self.wal_offset = index.offset.get();
		self.wal_generation = index.generation;
	}

	pub fn dirty(&self) -> bool {
		self.dirty != 0
	}

	pub fn set_dirty(&mut self, dirty: bool) {
		self.dirty = dirty as u8
	}
}

const HEADER_SIZE: usize = mem::size_of::<BufferedPageHeader>();
const BUFFERED_PAGE_SIZE: usize = PAGE_BODY_SIZE + HEADER_SIZE;

struct PageBuffer {
	buf: Option<NonNull<u8>>,
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
			buf: NonNull::new(buf),
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

	fn page_ptr(&self, index: usize) -> Option<NonNull<u8>> {
		if index >= self.num_pages {
			return None;
		}
		// Safety: the resulting pointer is guaranteed to be in the allocated buffer.
		Some(unsafe { self.buf?.add(index * BUFFERED_PAGE_SIZE) })
	}

	/// # Safety:
	/// The caller must ensure that no mutable reference to the same page
	/// exists.
	unsafe fn get_page(&self, index: usize) -> Option<&[u8]> {
		Some(std::slice::from_raw_parts(
			self.page_ptr(index)?.as_ptr(),
			BUFFERED_PAGE_SIZE,
		))
	}

	/// # Safety:
	/// The caller must ensure that no shared reference, and no other mutable
	/// references to the same page exist.
	unsafe fn get_page_mut(&self, index: usize) -> Option<&mut [u8]> {
		Some(std::slice::from_raw_parts_mut(
			self.page_ptr(index)?.as_ptr(),
			BUFFERED_PAGE_SIZE,
		))
	}
}

// Safety: The PageBuffer has no functionality that would make it unsafe
// to transfer across threads.
unsafe impl Send for PageBuffer {}

// Safety: The necessary conditions for safety are required from the caller of
// `get_page` and `get_page_mut`
unsafe impl Sync for PageBuffer {}

impl Drop for PageBuffer {
	fn drop(&mut self) {
		if let Some(buf) = self.buf {
			let buf_size = self.num_pages * BUFFERED_PAGE_SIZE;
			// Safety:
			// - `self.buf` is guaranteed not to be null
			// - The buffer is never reallocated, so the layout stays the same
			unsafe {
				dealloc(
					buf.as_ptr(),
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
		BufferedPageHeader::ref_from_bytes(&self.page[0..HEADER_SIZE]).unwrap()
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

#[cfg_attr(test, automock)]
pub(crate) trait PageWriteGuardApi {
	fn header(&self) -> &BufferedPageHeader;
	fn body(&self) -> &[u8];
	fn body_mut(&mut self) -> &mut [u8];
	fn header_mut(&mut self) -> &mut BufferedPageHeader;
	fn read(&self, offset: usize, buf: &mut [u8]);
	fn write(&mut self, offset: usize, buf: &[u8], wal_index: WalIndex);
}

impl<'a> PageWriteGuardApi for PageWriteGuard<'a> {
	fn header(&self) -> &BufferedPageHeader {
		BufferedPageHeader::ref_from_bytes(&self.page[0..HEADER_SIZE]).unwrap()
	}

	fn header_mut(&mut self) -> &mut BufferedPageHeader {
		BufferedPageHeader::mut_from_bytes(&mut self.page[0..HEADER_SIZE]).unwrap()
	}

	fn body(&self) -> &[u8] {
		&self.page[HEADER_SIZE..]
	}

	fn body_mut(&mut self) -> &mut [u8] {
		&mut self.page[HEADER_SIZE..]
	}

	fn read(&self, offset: usize, buf: &mut [u8]) {
		buf.copy_from_slice(&self.body()[offset..offset + buf.len()]);
	}

	fn write(&mut self, offset: usize, buf: &[u8], wal_index: WalIndex) {
		let header = self.header_mut();
		header.set_wal_index(wal_index);
		header.set_dirty(true);
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

pub(crate) struct PageCache<PS: PhysicalStorageApi = PhysicalStorage> {
	buf: Arc<PageBuffer>,
	physical_storage: Arc<PS>,
	thread_pool: Arc<ThreadPool>,
	indices: Arc<RwLock<HashMap<PageAddress, usize>>>,
	replacer: RwLock<CacheReplacer<PageAddress>>,
	scrap: Mutex<Vec<usize>>,
	has_scrap: AtomicBool,
	dirty_list: Arc<Mutex<Vec<PageAddress>>>,
	locks: Arc<Box<[RawRwLock]>>,
	max_num_dirty: usize,
	flush_timer_handle: TimerHandle,
}
assert_impl_all!(PageCache: Send, Sync);

// Safety: `buf`'s internal pointer is never leaked in any form.
unsafe impl<PS: PhysicalStorageApi + Send + Sync> Send for PageCache<PS> {}

// Safety: `buf` is only accessed through the `load` and `store` methods,
// which guarantee the safety of the references by acquiring the corresponding
// locks.
unsafe impl<PS: PhysicalStorageApi + Send + Sync> Sync for PageCache<PS> {}

struct DirtyPage<'a> {
	page_address: PageAddress,
	index: usize,
	guard: PageReadGuard<'a>,
}

impl<PS: PhysicalStorageApi + Send + Sync + 'static> PageCache<PS> {
	pub fn new(
		config: &PageCacheConfig,
		physical_storage: Arc<PS>,
		thread_pool: Arc<ThreadPool>,
	) -> Self {
		let num_pages = config.page_cache_size / BUFFERED_PAGE_SIZE;
		let buf = Arc::new(PageBuffer::new(num_pages));
		let replacer = CacheReplacer::new(num_pages);
		let indices = Arc::new(RwLock::new(HashMap::new()));
		let dirty_list = Arc::new(Mutex::new(Vec::new()));
		let locks = Arc::new(
			std::iter::repeat_with(|| RawRwLock::INIT)
				.take(num_pages)
				.collect(),
		);

		let (flush_timer, flush_timer_handle) = Timer::new(config.flush_period);
		thread_pool.spawn_ok(Self::periodic_flush_task(
			flush_timer,
			Arc::clone(&physical_storage),
			Arc::clone(&dirty_list),
			Arc::clone(&indices),
			Arc::clone(&locks),
			Arc::clone(&buf),
		));

		Self {
			buf,
			physical_storage,
			thread_pool,
			replacer: RwLock::new(replacer),
			indices,
			scrap: Mutex::new(Vec::new()),
			has_scrap: AtomicBool::new(false),
			dirty_list,
			locks,
			#[allow(clippy::cast_possible_truncation)]
			max_num_dirty: usize::max((num_pages as f32 * config.max_dirty_pages) as usize, 1),
			flush_timer_handle,
		}
	}

	fn evict_for(&self, page_address: PageAddress) -> Option<PageAddress> {
		let mut replacer = self.replacer.write();
		let mut maybe_evict = replacer.evict_replace(page_address);
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
				if evicted == page_address || self.locks[index].is_locked() {
					let mut replacer = self.replacer.write();
					maybe_evict = replacer.evict_replace(evicted);
					continue;
				}
			}
			break;
		}
		maybe_evict
	}

	fn get_store_index(&self, page_address: PageAddress) -> usize {
		let indices = self.indices.read();
		if let Some(stored_index) = indices.get(&page_address).copied() {
			return stored_index;
		}
		mem::drop(indices);

		if self.has_scrap.load(Ordering::Relaxed) {
			let mut scrap = self.scrap.lock();
			if let Some(scrap_index) = scrap.pop() {
				return scrap_index;
			}
		}

		let maybe_evict = self.evict_for(page_address);
		let mut indices = self.indices.write();
		if let Some(evict) = maybe_evict {
			let index = indices
				.remove(&evict)
				.expect("Tried to evict a page that is not in the cache!");
			indices.insert(page_address, index);
			index
		} else {
			let index = self
				.buf
				.push_page()
				.expect("Failed to evict a page when the buffer was full!");
			indices.insert(page_address, index);
			index
		}
	}

	fn get_load_index(&self, page_address: PageAddress) -> Option<usize> {
		let indices = self.indices.read();
		let index = indices.get(&page_address).copied()?;
		mem::drop(indices);

		let replacer = self.replacer.read();
		let access_successful = replacer.access(&page_address);
		debug_assert!(access_successful);
		mem::drop(replacer);

		Some(index)
	}

	fn load_direct<'a>(
		locks: &'a [RawRwLock],
		buf: &'a PageBuffer,
		index: usize,
	) -> PageReadGuard<'a> {
		let lock = &locks[index];
		lock.lock_shared();
		// Safety: The safety of the reference is guaranteed by acquiring the shared
		// lock.
		let page =
			unsafe { buf.get_page(index) }.expect("Tried to index page buffer out of bounds!");

		PageReadGuard {
			lock,
			page,
			_marker: PhantomData,
		}
	}

	fn load_mut_direct<'a>(
		locks: &'a [RawRwLock],
		buf: &'a PageBuffer,
		index: usize,
	) -> PageWriteGuard<'a> {
		let lock = &locks[index];
		lock.lock_exclusive();
		// Safety: The safety of the reference is guaranteed by acquiring the exclusive
		// lock.
		let page =
			unsafe { buf.get_page_mut(index) }.expect("Triet to index page buffer out of bounds!");

		PageWriteGuard {
			index,
			lock,
			page,
			_marker: PhantomData,
		}
	}

	fn flush(
		physical_storage: &PS,
		dirty_list: &Mutex<Vec<PageAddress>>,
		indices: &RwLock<HashMap<PageAddress, usize>>,
		locks: &[RawRwLock],
		buf: &PageBuffer,
	) -> Result<(), StorageError> {
		let mut dirty_list_guard = dirty_list.lock();
		let dirty_list_copy = dirty_list_guard.clone();
		dirty_list_guard.clear();
		mem::drop(dirty_list_guard);

		let mut error: Option<StorageError> = None;
		let mut dirty_pages: Vec<DirtyPage> = Vec::with_capacity(dirty_list_copy.len());

		for page_address in dirty_list_copy.iter() {
			let indices = indices.read();
			let Some(index) = indices.get(page_address).copied() else {
				continue;
			};
			mem::drop(indices);

			let guard = Self::load_direct(locks, buf, index);
			if !guard.header().dirty() {
				continue;
			}

			dirty_pages.push(DirtyPage {
				page_address: *page_address,
				index,
				guard,
			});
		}

		let ops: Vec<Op> = dirty_pages
			.iter()
			.map(|dp| {
				Op::Write(WriteOp {
					wal_index: dp.guard.header().wal_index(),
					page_address: dp.page_address,
					buf: dp.guard.body(),
				})
			})
			.collect();

		if let Err(err) = physical_storage.batch(ops.into()) {
			error = Some(err);
		}

		for dirty_page in dirty_pages.into_iter() {
			mem::drop(dirty_page.guard);
			let mut guard_mut = Self::load_mut_direct(locks, buf, dirty_page.index);
			guard_mut.header_mut().set_dirty(false);
		}

		if let Some(err) = error {
			let mut dirty_list_guard = dirty_list.lock();
			dirty_list_guard.extend(&dirty_list_copy);
			return Err(err);
		}

		Ok(())
	}

	async fn flush_ok(
		physical_storage: &PS,
		dirty_list: &Mutex<Vec<PageAddress>>,
		indices: &RwLock<HashMap<PageAddress, usize>>,
		locks: &[RawRwLock],
		buf: &PageBuffer,
	) {
		if let Err(err) = Self::flush(physical_storage, dirty_list, indices, locks, buf) {
			error!("Page cache flush failed: {err}");
		}
	}

	async fn single_flush_task(
		physical_storage: Arc<PS>,
		dirty_list: Arc<Mutex<Vec<PageAddress>>>,
		indices: Arc<RwLock<HashMap<PageAddress, usize>>>,
		locks: Arc<Box<[RawRwLock]>>,
		buf: Arc<PageBuffer>,
	) {
		Self::flush_ok(&physical_storage, &dirty_list, &indices, &locks, &buf).await;
	}

	async fn periodic_flush_task(
		timer: Timer,
		physical_storage: Arc<PS>,
		dirty_list: Arc<Mutex<Vec<PageAddress>>>,
		indices: Arc<RwLock<HashMap<PageAddress, usize>>>,
		locks: Arc<Box<[RawRwLock]>>,
		buf: Arc<PageBuffer>,
	) {
		while timer.wait() {
			Self::flush_ok(&physical_storage, &dirty_list, &indices, &locks, &buf).await;
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

	fn has_page(&self, page_address: PageAddress) -> bool;
	fn load<'a>(&'a self, page_address: PageAddress) -> Option<Self::ReadGuard<'a>>;
	fn load_mut<'a>(&'a self, page_address: PageAddress) -> Option<Self::WriteGuard<'a>>;
	fn store<'a>(&'a self, page_address: PageAddress) -> Self::WriteGuard<'a>;
	fn flush(&self);
	fn flush_sync(&self) -> Result<(), StorageError>;
	fn scrap(&self, page_address: PageAddress);
	fn downgrade_guard<'a>(&'a self, guard: Self::WriteGuard<'a>) -> Self::ReadGuard<'a>;
}

impl<PS: PhysicalStorageApi + Send + Sync + 'static> PageCacheApi for PageCache<PS> {
	type ReadGuard<'a> = PageReadGuard<'a>;
	type WriteGuard<'a> = PageWriteGuard<'a>;

	fn has_page(&self, page_address: PageAddress) -> bool {
		let indices = self.indices.read();
		indices.contains_key(&page_address)
	}

	fn load(&self, page_address: PageAddress) -> Option<PageReadGuard<'_>> {
		let index = self.get_load_index(page_address)?;
		Some(Self::load_direct(&self.locks, &self.buf, index))
	}

	fn load_mut(&self, page_address: PageAddress) -> Option<Self::WriteGuard<'_>> {
		let index = self.get_load_index(page_address)?;
		Some(Self::load_mut_direct(&self.locks, &self.buf, index))
	}

	fn store(&self, page_address: PageAddress) -> PageWriteGuard<'_> {
		let mut dirty_list = self.dirty_list.lock();
		dirty_list.push(page_address);
		if dirty_list.len() >= self.max_num_dirty {
			self.thread_pool.spawn_ok(Self::single_flush_task(
				Arc::clone(&self.physical_storage),
				Arc::clone(&self.dirty_list),
				Arc::clone(&self.indices),
				Arc::clone(&self.locks),
				Arc::clone(&self.buf),
			));
		}
		mem::drop(dirty_list);

		let index = self.get_store_index(page_address);
		Self::load_mut_direct(&self.locks, &self.buf, index)
	}

	fn flush(&self) {
		let physical_storage = Arc::clone(&self.physical_storage);
		let dirty_list = Arc::clone(&self.dirty_list);
		let indices = Arc::clone(&self.indices);
		let locks = Arc::clone(&self.locks);
		let buf = Arc::clone(&self.buf);
		self.thread_pool.spawn_ok(Self::single_flush_task(
			physical_storage,
			dirty_list,
			indices,
			locks,
			buf,
		))
	}

	fn flush_sync(&self) -> Result<(), StorageError> {
		Self::flush(
			&self.physical_storage,
			&self.dirty_list,
			&self.indices,
			&self.locks,
			&self.buf,
		)
	}

	fn scrap(&self, page_address: PageAddress) {
		let mut indices = self.indices.write();
		let Some(index) = indices.remove(&page_address) else {
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
}

#[cfg(test)]
mod tests {
	use pretty_assertions::assert_buf_eq;

	use crate::{
		page_store::{
			physical::MockPhysicalStorageApi,
			test_helpers::{page_address, wal_index},
		},
		utils::units::MIB,
	};

	use super::*;

	#[test]
	fn load_and_store() {
		// given
		let cache = PageCache::new(
			&PageCacheConfig {
				page_cache_size: 2 * MIB,
				..Default::default()
			},
			Arc::new(MockPhysicalStorageApi::new()),
			Arc::new(ThreadPool::new().unwrap()),
		);

		// when
		let expected_page = [69; PAGE_BODY_SIZE];
		cache
			.store(page_address!(69, 420))
			.write(0, &expected_page, wal_index!(1, 2));

		let mut received_page = [0; PAGE_BODY_SIZE];
		cache
			.load(page_address!(69, 420))
			.unwrap()
			.read(0, &mut received_page);

		// then
		assert_buf_eq!(expected_page, received_page);
	}

	#[test]
	fn load_cache_miss() {
		// given
		let cache = PageCache::new(
			&PageCacheConfig {
				page_cache_size: 2 * MIB,
				..Default::default()
			},
			Arc::new(MockPhysicalStorageApi::new()),
			Arc::new(ThreadPool::new().unwrap()),
		);

		// when
		let guard = cache.load(page_address!(69, 420));

		// then
		assert!(guard.is_none())
	}

	#[test]
	fn evict_correct_page() {
		// given
		let cache = PageCache::new(
			&PageCacheConfig {
				page_cache_size: 4 * BUFFERED_PAGE_SIZE,
				..Default::default()
			},
			Arc::new(MockPhysicalStorageApi::new()),
			Arc::new(ThreadPool::new().unwrap()),
		);

		// when
		cache.store(page_address!(1, 1)); // add 1, 1 to recent
		cache.store(page_address!(2, 2)); // add 2, 2 to recent
		cache.store(page_address!(3, 3)); // add 3, 3 to recent
		cache.store(page_address!(4, 4)); // add 4, 4 to recent
		cache.load(page_address!(1, 1)); // 1, 1 was referenced in recent
		cache.load(page_address!(2, 2)); // 2, 2 was referenced in recent
		cache.load(page_address!(1, 1)); // 1, 1 is promoted to frequent

		// recent is large, therefore 3, 3 is evicted as it is the first
		// non-referenced item in frequent
		cache.store(page_address!(5, 5));

		// then
		assert!(cache.load(page_address!(1, 1)).is_some());
		assert!(cache.load(page_address!(2, 2)).is_some());
		assert!(cache.load(page_address!(3, 3)).is_none());
		assert!(cache.load(page_address!(4, 4)).is_some());
		assert!(cache.load(page_address!(5, 5)).is_some());
	}

	#[test]
	fn doesnt_evict_locked_page() {
		// given
		let cache = PageCache::new(
			&PageCacheConfig {
				page_cache_size: 4 * BUFFERED_PAGE_SIZE,
				..Default::default()
			},
			Arc::new(MockPhysicalStorageApi::new()),
			Arc::new(ThreadPool::new().unwrap()),
		);

		// when
		cache.store(page_address!(1, 1)); // add 1, 1 to recent
		cache.store(page_address!(2, 2)); // add 2, 2 to recent
		let guard = cache.store(page_address!(3, 3)); // add 3, 3 to recent
		cache.store(page_address!(4, 4)); // add 4, 4 to recent
		cache.load(page_address!(1, 1)); // 1, 1 was referenced in recent
		cache.load(page_address!(2, 2)); // 2, 2 was referenced in recent
		cache.load(page_address!(1, 1)); // 1, 1 is promoted to frequent

		// recent is large, therefore 3, 3 would be evicted, but it is locked, so 4, 4
		// is evicted instead
		cache.store(page_address!(5, 5));

		mem::drop(guard);

		// then
		assert!(cache.load(page_address!(1, 1)).is_some());
		assert!(cache.load(page_address!(2, 2)).is_some());
		assert!(cache.load(page_address!(3, 3)).is_some());
		assert!(cache.load(page_address!(4, 4)).is_none());
		assert!(cache.load(page_address!(5, 5)).is_some());
	}
}
