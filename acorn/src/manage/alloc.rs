use std::{mem, num::NonZeroU16, sync::Arc};

use parking_lot::RwLock;

use crate::{index::PageId, utils::array_map::ArrayMap};

use super::{err::Error, rw::PageRwManager, segment_alloc::SegmentAllocManager};

pub struct AllocManager {
	segments: RwLock<ArrayMap<SegmentAllocManager>>,
	free_stack: RwLock<Vec<u32>>,
	rw_mgr: Arc<PageRwManager>,
}

impl AllocManager {
	pub fn new(rw_mgr: Arc<PageRwManager>) -> Self {
		Self {
			segments: RwLock::new(ArrayMap::new()),
			free_stack: RwLock::new(Vec::new()),
			rw_mgr,
		}
	}

	pub fn free_page(&self, tid: u64, page_id: PageId) -> Result<(), Error> {
		let Some(page_num) = NonZeroU16::new(page_id.page_num) else {
			return Ok(());
		};

		self.ensure_segment_alloc_exists(page_id.segment_num);

		let segments = self.segments.read();
		let segment = segments.get(page_id.segment_num as usize).unwrap();
		segment.free_page(tid, page_num)
	}

	pub fn alloc_page(&self, tid: u64) -> Result<PageId, Error> {
		if let Some(page_id) = self.alloc_from_free_stack(tid)? {
			return Ok(page_id);
		}
		self.alloc_search(tid)
	}

	fn alloc_search(&self, tid: u64) -> Result<PageId, Error> {
		for segment_num in 0_u32.. {
			if let Some(page_id) = self.try_alloc_in_segment(tid, segment_num)? {
				return Ok(page_id);
			}
		}
		Err(Error::SizeLimitReached)
	}

	fn alloc_from_free_stack(&self, tid: u64) -> Result<Option<PageId>, Error> {
		loop {
			let Some(segment_num) = self.peek_free_stack() else {
				return Ok(None);
			};
			let Some(page_id) = self.try_alloc_in_segment(tid, segment_num)? else {
				self.pop_free_stack();
				continue;
			};
			return Ok(Some(page_id));
		}
	}

	fn try_alloc_in_segment(&self, tid: u64, segment_num: u32) -> Result<Option<PageId>, Error> {
		self.ensure_segment_alloc_exists(segment_num);

		let segments = self.segments.read();
		let segment = segments.get(segment_num as usize).unwrap();

		Ok(segment
			.alloc_page(tid)?
			.map(|page_num| PageId::new(segment_num, page_num.get())))
	}

	fn pop_free_stack(&self) {
		let mut free_stack = self.free_stack.write();
		free_stack.pop();
	}

	fn peek_free_stack(&self) -> Option<u32> {
		let free_stack = self.free_stack.read();
		free_stack.last().copied()
	}

	fn ensure_segment_alloc_exists(&self, segment_num: u32) {
		let segments = self.segments.read();
		if segments.has(segment_num as usize) {
			return;
		}
		mem::drop(segments);

		let mut segments = self.segments.write();
		let segment_alloc = SegmentAllocManager::new(Arc::clone(&self.rw_mgr), segment_num);
		segments.insert(segment_num as usize, segment_alloc);
	}
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use crate::{
		cache::PageCache,
		disk::{self, DiskStorage},
		manage::transaction::TransactionManager,
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new());
		let rw_mgr = Arc::new(PageRwManager::new(
			Arc::clone(&cache),
			Arc::clone(&transaction_mgr),
		));
		let alloc_mgr = AllocManager::new(Arc::clone(&rw_mgr));

		let tid = transaction_mgr.begin().unwrap();
		let page_id = alloc_mgr.alloc_page(tid).unwrap();
		transaction_mgr.commit(tid).unwrap();

		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_free_page() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new());
		let rw_mgr = Arc::new(PageRwManager::new(
			Arc::clone(&cache),
			Arc::clone(&transaction_mgr),
		));
		let alloc_mgr = AllocManager::new(Arc::clone(&rw_mgr));

		let tid = transaction_mgr.begin().unwrap();
		let page_id = alloc_mgr.alloc_page(tid).unwrap();
		transaction_mgr.commit(tid).unwrap();

		let tid = transaction_mgr.begin().unwrap();
		alloc_mgr.free_page(tid, page_id).unwrap();
		transaction_mgr.commit(tid).unwrap();
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_free_realloc_page() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new());
		let rw_mgr = Arc::new(PageRwManager::new(
			Arc::clone(&cache),
			Arc::clone(&transaction_mgr),
		));
		let alloc_mgr = AllocManager::new(Arc::clone(&rw_mgr));

		let tid = transaction_mgr.begin().unwrap();
		let page_id = alloc_mgr.alloc_page(tid).unwrap();
		transaction_mgr.commit(tid).unwrap();

		let tid = transaction_mgr.begin().unwrap();
		alloc_mgr.free_page(tid, page_id).unwrap();
		transaction_mgr.commit(tid).unwrap();

		let tid = transaction_mgr.begin().unwrap();
		let page_id_2 = alloc_mgr.alloc_page(tid).unwrap();
		transaction_mgr.commit(tid).unwrap();

		assert_eq!(page_id_2, PageId::new(0, 1));
	}
}
