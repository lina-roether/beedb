use std::{mem, num::NonZeroU16, sync::Arc};

use parking_lot::RwLock;

use crate::{id::PageId, utils::array_map::ArrayMap};

use super::{
	err::Error,
	segment_alloc::SegmentAllocManager,
	transaction::{Transaction, TransactionManager},
};

pub struct AllocManager {
	segments: RwLock<ArrayMap<SegmentAllocManager>>,
	free_stack: RwLock<Vec<u32>>,
	tm: Arc<TransactionManager>,
}

impl AllocManager {
	pub fn new(tm: Arc<TransactionManager>) -> Self {
		Self {
			segments: RwLock::new(ArrayMap::new()),
			free_stack: RwLock::new(Vec::new()),
			tm,
		}
	}

	pub fn free_page(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error> {
		let Some(page_num) = NonZeroU16::new(page_id.page_num) else {
			return Ok(());
		};

		self.ensure_segment_alloc_exists(page_id.segment_num);

		let segments = self.segments.read();
		let segment = segments.get(page_id.segment_num as usize).unwrap();
		segment.free_page(t, page_num)
	}

	pub fn alloc_page(&self, t: &mut Transaction) -> Result<PageId, Error> {
		if let Some(page_id) = self.alloc_from_free_stack(t)? {
			return Ok(page_id);
		}
		self.alloc_search(t)
	}

	fn alloc_search(&self, t: &mut Transaction) -> Result<PageId, Error> {
		let mut free_stack = self.free_stack.write();
		for segment_num in 0_u32.. {
			if let Some(page_id) = self.try_alloc_in_segment(t, segment_num)? {
				free_stack.push(segment_num);
				return Ok(page_id);
			}
		}
		Err(Error::SizeLimitReached)
	}

	fn alloc_from_free_stack(&self, t: &mut Transaction) -> Result<Option<PageId>, Error> {
		loop {
			let Some(segment_num) = self.peek_free_stack() else {
				return Ok(None);
			};
			let Some(page_id) = self.try_alloc_in_segment(t, segment_num)? else {
				self.pop_free_stack();
				continue;
			};
			return Ok(Some(page_id));
		}
	}

	fn try_alloc_in_segment(
		&self,
		t: &mut Transaction,
		segment_num: u32,
	) -> Result<Option<PageId>, Error> {
		self.ensure_segment_alloc_exists(segment_num);

		let segments = self.segments.read();
		let segment = segments.get(segment_num as usize).unwrap();

		Ok(segment
			.alloc_page(t)?
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
		let segment_alloc = SegmentAllocManager::new(Arc::clone(&self.tm), segment_num);
		segments.insert(segment_num as usize, segment_alloc);
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::tempdir;
	use test::Bencher;

	use crate::{
		cache::PageCache,
		disk::{self, DiskStorage},
		manage::transaction::TransactionManager,
		wal::{self, Wal},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm));

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[bench]
	#[cfg_attr(miri, ignore)]
	fn bench_alloc_page(b: &mut Bencher) {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm));

		b.iter(|| {
			let mut t = tm.begin();
			alloc_mgr.alloc_page(&mut t).unwrap();
			t.commit().unwrap();
		});
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_free_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm));

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		alloc_mgr.free_page(&mut t, page_id).unwrap();
		t.commit().unwrap();
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_free_realloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm));

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		alloc_mgr.free_page(&mut t, page_id).unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		let page_id_2 = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		assert_eq!(page_id_2, PageId::new(0, 1));
	}
}
