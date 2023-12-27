use std::{collections::HashMap, mem, num::NonZeroU32};

use parking_lot::Mutex;

use self::{
	buffer::{PageBuffer, PageReadGuard, PageWriteGuard},
	manager::CacheManager,
};

use crate::io::{IoTarget, Storage, StorageError};

mod buffer;
mod manager;

pub struct PageCache<'a, T: IoTarget> {
	state: Mutex<CacheState>,
	buffer: PageBuffer,
	storage: &'a Storage<T>,
}

impl<'a, T: IoTarget> PageCache<'a, T> {
	pub fn new(storage: &'a Storage<T>, page_size: usize, length: usize) -> Self {
		Self {
			state: Mutex::new(CacheState {
				manager: CacheManager::new(length),
				map: HashMap::new(),
			}),
			buffer: PageBuffer::new(page_size, length),
			storage,
		}
	}

	pub fn read_page(&self, page_number: NonZeroU32) -> Result<PageReadGuard, StorageError> {
		let index = self.access(page_number)?;
		Ok(self.buffer.read_page(index).unwrap())
	}

	pub fn write_page(&self, page_number: NonZeroU32) -> Result<PageWriteGuard, StorageError> {
		let index = self.access(page_number)?;
		Ok(self.buffer.write_page(index).unwrap())
	}

	fn access(&self, page_number: NonZeroU32) -> Result<usize, StorageError> {
		let mut state = self.state.lock();
		state.manager.access(page_number);

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
			self.buffer.free_page(index);
		}

		let index = self
			.buffer
			.allocate_page()
			.expect("Failed to allocate a page in the page cache");

		let mut page = self.buffer.write_page(index).unwrap();
		self.storage.read_page(&mut page, page_number)?;
		mem::drop(page);

		Ok(index)
	}
}

struct CacheState {
	manager: CacheManager,
	map: HashMap<NonZeroU32, usize>,
}
