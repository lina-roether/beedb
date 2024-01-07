use std::{
	collections::{HashMap, HashSet},
	mem,
};

use parking_lot::Mutex;
use static_assertions::assert_impl_all;

use self::{buffer::PageBuffer, manager::CacheManager};

use crate::{
	disk::{self, DiskStorage},
	index::PageId,
};

mod buffer;
mod manager;

pub use buffer::{PageReadGuard, PageWriteGuard};

pub struct PageCache {
	state: Mutex<CacheState>,
	buffer: PageBuffer,
	storage: DiskStorage,
}

assert_impl_all!(PageCache: Send, Sync);

impl PageCache {
	pub fn new(storage: DiskStorage, length: usize) -> Self {
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

	pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, disk::Error> {
		let index = self.access(page_id, false)?;
		Ok(self.buffer.read_page(index).unwrap())
	}

	pub fn write_page(&self, page_id: PageId) -> Result<PageWriteGuard, disk::Error> {
		let index = self.access(page_id, true)?;
		Ok(self.buffer.write_page(index).unwrap())
	}

	#[inline]
	pub fn num_dirty(&self) -> usize {
		self.state.lock().dirty.len()
	}

	pub fn flush(&self) -> Result<(), disk::Error> {
		let mut state = self.state.lock();
		for dirty_page in state.dirty.iter().copied() {
			let index = *state.map.get(&dirty_page).unwrap();
			let page = self.buffer.read_page(index).unwrap();
			self.storage.write_page(&page, dirty_page)?;
		}
		state.dirty.clear();
		Ok(())
	}

	fn access(&self, page_id: PageId, dirty: bool) -> Result<usize, disk::Error> {
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
	use tempfile::tempdir;

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn simple_read_write() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let cache = PageCache::new(storage, 128);

		{
			let mut page_1 = cache.write_page(PageId::new(0, 1)).unwrap();
			page_1.fill(69);
		}

		{
			let mut page_2 = cache.write_page(PageId::new(0, 2)).unwrap();
			page_2.fill(25);
		}

		assert_eq!(cache.num_dirty(), 2);
		cache.flush().unwrap();
		assert_eq!(cache.num_dirty(), 0);

		let page_1 = cache.read_page(PageId::new(0, 1)).unwrap();
		let page_2 = cache.read_page(PageId::new(0, 2)).unwrap();

		assert!(page_1.iter().all(|b| *b == 69));
		assert!(page_2.iter().all(|b| *b == 25));
	}
}
