use std::{
	collections::{HashMap, HashSet},
	mem,
};

use parking_lot::Mutex;
use static_assertions::assert_impl_all;

use self::{buffer::PageBuffer, manager::CacheManager};

use crate::{
	disk::storage::{self, Storage, StorageApi},
	id::PageId,
};

mod buffer;
mod manager;

pub(crate) use buffer::{PageReadGuard, PageWriteGuard};

pub(crate) struct PageCache<Storage>
where
	Storage: StorageApi,
{
	state: Mutex<CacheState>,
	buffer: PageBuffer,
	storage: Storage,
}

assert_impl_all!(PageCache<Storage>: Send, Sync);

impl<Storage> PageCache<Storage>
where
	Storage: StorageApi,
{
	pub fn new(storage: Storage, length: usize) -> Self {
		Self {
			state: Mutex::new(CacheState {
				manager: CacheManager::new(length),
				map: HashMap::new(),
				dirty: HashSet::new(),
			}),
			buffer: PageBuffer::new(storage.page_size().into(), length),
			storage,
		}
	}

	pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, storage::Error> {
		let index = self.access(page_id, false)?;
		Ok(self.buffer.read_page(index).unwrap())
	}

	pub fn write_page(&self, page_id: PageId) -> Result<PageWriteGuard, storage::Error> {
		let index = self.access(page_id, true)?;
		Ok(self.buffer.write_page(index).unwrap())
	}

	#[inline]
	pub fn num_dirty(&self) -> usize {
		self.state.lock().dirty.len()
	}

	#[inline]
	pub fn segment_nums(&self) -> Box<[u32]> {
		self.storage.segment_nums()
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.storage.page_size()
	}

	pub fn flush(&self) -> Result<(), storage::Error> {
		let mut state = self.state.lock();
		for dirty_page in state.dirty.iter().copied() {
			let index = *state.map.get(&dirty_page).unwrap();
			let page = self.buffer.read_page(index).unwrap();
			self.storage.write_page(&page, dirty_page)?;
		}
		state.dirty.clear();
		Ok(())
	}

	fn access(&self, page_id: PageId, dirty: bool) -> Result<usize, storage::Error> {
		let mut state = self.state.lock();
		state.manager.access(page_id);

		if dirty && !state.dirty.contains(&page_id) {
			state.dirty.insert(page_id);
		}

		if let Some(&index) = state.map.get(&page_id) {
			return Ok(index);
		}

		if !self.buffer.has_space() {
			let reclaimed_page = state
				.manager
				.reclaim()
				.expect("Page cache failed to reclaim required memory");
			let index = state
				.map
				.remove(&reclaimed_page)
				.expect("Tried to reclaim an unused page slot");
			if state.dirty.contains(&reclaimed_page) {
				let page = self.buffer.read_page(index).unwrap();
				self.storage.write_page(&page, reclaimed_page)?;
				state.dirty.remove(&reclaimed_page);
			}
			self.buffer.free_page(index);
		}

		let index = self
			.buffer
			.allocate_page()
			.expect("Failed to allocate a page in the page cache");

		let mut page = self.buffer.write_page(index).unwrap();
		self.storage.read_page(&mut page, page_id)?;
		mem::drop(page);

		state.map.insert(page_id, index);

		Ok(index)
	}
}

struct CacheState {
	manager: CacheManager,
	map: HashMap<PageId, usize>,
	dirty: HashSet<PageId>,
}

#[cfg(test)]
mod tests {
	use crate::cache::tests::storage::MockStorageApi;
	use mockall::predicate::*;

	use super::*;

	#[test]
	fn simple_read_write() {
		// given
		let mut storage = MockStorageApi::new();
		storage.expect_page_size().returning(|| 8);
		storage
			.expect_read_page()
			.with(always(), eq(PageId::new(0, 1)))
			.times(1)
			.returning(|_, _| Ok(()));
		storage
			.expect_read_page()
			.with(always(), eq(PageId::new(0, 2)))
			.times(1)
			.returning(|_, _| Ok(()));
		storage.expect_write_page().never();

		// when
		let cache = PageCache::new(storage, 128);

		let mut page_1 = cache.write_page(PageId::new(0, 1)).unwrap();
		page_1.fill(69);
		mem::drop(page_1);

		let mut page_2 = cache.write_page(PageId::new(0, 2)).unwrap();
		page_2.fill(25);
		mem::drop(page_2);

		let page_1 = cache.read_page(PageId::new(0, 1)).unwrap();
		let page_2 = cache.read_page(PageId::new(0, 2)).unwrap();

		// then
		assert_eq!(cache.num_dirty(), 2);
		assert!(page_1.iter().all(|b| *b == 69));
		assert!(page_2.iter().all(|b| *b == 25));
	}

	#[test]
	fn flush_writes() {
		// given
		let mut storage = MockStorageApi::new();
		storage.expect_page_size().returning(|| 8);
		storage
			.expect_read_page()
			.with(always(), eq(PageId::new(0, 1)))
			.times(1)
			.returning(|_, _| Ok(()));

		// expect
		storage
			.expect_write_page()
			.with(eq([69; 8]), eq(PageId::new(0, 1)))
			.times(1)
			.returning(|_, _| Ok(()));

		// when
		let cache = PageCache::new(storage, 128);

		let mut page_1 = cache.write_page(PageId::new(0, 1)).unwrap();
		page_1.fill(69);
		mem::drop(page_1);

		cache.flush().unwrap();

		// then
		assert_eq!(cache.num_dirty(), 0);
	}
}
