use std::{
	collections::{HashMap, HashSet},
	mem, usize,
};

use parking_lot::Mutex;

use self::{
	buffer::{PageBuffer, PageReadGuard, PageWriteGuard},
	manager::CacheManager,
};

use crate::storage::{IoTarget, PageNumber, Storage, StorageError};

mod buffer;
mod manager;

pub struct PageCache<'a, T: IoTarget> {
	state: Mutex<CacheState>,
	buffer: PageBuffer,
	storage: &'a Storage<T>,
}

impl<'a, T: IoTarget> PageCache<'a, T> {
	pub fn new(storage: &'a Storage<T>, length: usize) -> Self {
		Self {
			state: Mutex::new(CacheState {
				manager: CacheManager::new(length),
				map: HashMap::new(),
				dirty: HashSet::new(),
			}),
			buffer: PageBuffer::new(storage.page_size(), length),
			storage,
		}
	}

	pub fn read_page(&self, page_number: PageNumber) -> Result<PageReadGuard, StorageError> {
		let index = self.access(page_number, false)?;
		Ok(self.buffer.read_page(index).unwrap())
	}

	pub fn write_page(&self, page_number: PageNumber) -> Result<PageWriteGuard, StorageError> {
		let index = self.access(page_number, true)?;
		Ok(self.buffer.write_page(index).unwrap())
	}

	#[inline]
	pub fn num_dirty(&self) -> usize {
		self.state.lock().dirty.len()
	}

	pub fn flush(&self) -> Result<(), StorageError> {
		let mut state = self.state.lock();
		for dirty_page in state.dirty.iter().copied() {
			let index = *state.map.get(&dirty_page).unwrap();
			let page = self.buffer.read_page(index).unwrap();
			self.storage.write_page(&page, dirty_page)?;
		}
		state.dirty.clear();
		Ok(())
	}

	fn access(&self, page_number: PageNumber, dirty: bool) -> Result<usize, StorageError> {
		let mut state = self.state.lock();
		state.manager.access(page_number);

		if dirty && !state.dirty.contains(&page_number) {
			state.dirty.insert(page_number);
		}

		if let Some(&index) = state.map.get(&page_number) {
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
		self.storage.read_page(&mut page, page_number)?;
		mem::drop(page);

		state.map.insert(page_number, index);

		Ok(index)
	}
}

struct CacheState {
	manager: CacheManager,
	map: HashMap<PageNumber, usize>,
	dirty: HashSet<PageNumber>,
}

#[cfg(test)]
mod tests {
	use crate::storage::InitParams;

	use super::*;

	#[test]
	fn simple_read_write() {
		let mut file = Vec::new();
		Storage::init(&mut file, InitParams::default()).unwrap();
		let storage = Storage::load(file).unwrap();
		let cache = PageCache::new(&storage, 128);

		let page_num_1 = storage.allocate_page().unwrap();
		let page_num_2 = storage.allocate_page().unwrap();

		{
			let mut page_1 = cache.write_page(page_num_1).unwrap();
			page_1.fill(69);
		}

		{
			let mut page_2 = cache.write_page(page_num_2).unwrap();
			page_2.fill(25);
		}

		assert_eq!(cache.num_dirty(), 2);
		cache.flush().unwrap();
		assert_eq!(cache.num_dirty(), 0);

		let page_1 = cache.read_page(page_num_1).unwrap();
		let page_2 = cache.read_page(page_num_2).unwrap();

		assert!(page_1.iter().all(|b| *b == 69));
		assert!(page_2.iter().all(|b| *b == 25));
	}
}
