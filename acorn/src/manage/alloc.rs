use std::{collections::HashSet, num::NonZeroU16, sync::Arc};

use parking_lot::Mutex;

use crate::{id::PageId, utils::array_map::ArrayMap};

use super::{err::Error, read::ReadManager, segment::SegmentManager, transaction::Transaction};

pub struct AllocManager {
	state: Mutex<State>,
	rm: Arc<ReadManager>,
}

impl AllocManager {
	pub fn new(rm: Arc<ReadManager>) -> Result<Self, Error> {
		let mut state = State {
			segments: ArrayMap::new(),
			free_cache: HashSet::new(),
			next_segment: 0,
		};
		for segment_num in rm.segment_nums().iter() {
			state.add_segment(*segment_num, Arc::clone(&rm))?;
		}

		Ok(Self {
			state: Mutex::new(state),
			rm,
		})
	}

	pub fn free_page(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error> {
		let mut state = self.state.lock();

		let Some(page_num) = NonZeroU16::new(page_id.page_num) else {
			return Ok(());
		};

		let Some(segment) = state.get_segment(page_id.segment_num) else {
			return Ok(());
		};

		segment.free_page(t, page_num)?;
		state.free_cache_store(page_id.segment_num);
		Ok(())
	}

	pub fn alloc_page(&self, t: &mut Transaction) -> Result<PageId, Error> {
		if let Some(page_id) = self.alloc_from_free_cache(t)? {
			return Ok(page_id);
		}
		self.alloc_in_new_segment(t)
	}

	fn alloc_in_new_segment(&self, t: &mut Transaction) -> Result<PageId, Error> {
		let mut state = self.state.lock();
		let next_segment = state.next_segment;
		let Some(page_id) = state.try_alloc_in_new(t, next_segment, Arc::clone(&self.rm))? else {
			return Err(Error::SizeLimitReached);
		};
		Ok(page_id)
	}

	fn alloc_from_free_cache(&self, t: &mut Transaction) -> Result<Option<PageId>, Error> {
		let mut state = self.state.lock();
		loop {
			let Some(segment_num) = state.free_cache_get() else {
				return Ok(None);
			};
			let Some(page_id) = state.try_alloc_in_existing(t, segment_num)? else {
				state.free_cache_evict(segment_num);
				continue;
			};
			return Ok(Some(page_id));
		}
	}
}

struct State {
	segments: ArrayMap<SegmentManager>,
	free_cache: HashSet<u32>,
	next_segment: u32,
}

impl State {
	fn try_alloc_in(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
		rm: Arc<ReadManager>,
	) -> Result<Option<PageId>, Error> {
		if self.has_segment(segment_num) {
			self.try_alloc_in_existing(t, segment_num)
		} else {
			self.try_alloc_in_new(t, segment_num, rm)
		}
	}

	fn try_alloc_in_existing(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
	) -> Result<Option<PageId>, Error> {
		let Some(segment) = self.get_segment(segment_num) else {
			return Ok(None);
		};
		let Some(page_num) = segment.alloc_page(t)? else {
			return Ok(None);
		};
		Ok(Some(PageId::new(segment_num, page_num.get())))
	}

	fn try_alloc_in_new(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
		rm: Arc<ReadManager>,
	) -> Result<Option<PageId>, Error> {
		let segment = self.add_segment(segment_num, rm)?;
		let Some(page_num) = segment.alloc_page(t)? else {
			return Ok(None);
		};
		Ok(Some(PageId::new(segment_num, page_num.get())))
	}

	fn has_segment(&self, segment_num: u32) -> bool {
		self.segments.has(segment_num as usize)
	}

	fn add_segment(
		&mut self,
		segment_num: u32,
		rm: Arc<ReadManager>,
	) -> Result<&mut SegmentManager, Error> {
		let segment_alloc = SegmentManager::new(rm, segment_num)?;
		if segment_alloc.has_free_pages() {
			self.free_cache.insert(segment_num);
		}
		if segment_num >= self.next_segment {
			self.next_segment = segment_num + 1;
		}
		self.segments.insert(segment_num as usize, segment_alloc);
		Ok(self.segments.get_mut(segment_num as usize).unwrap())
	}

	fn get_segment(&mut self, segment_num: u32) -> Option<&mut SegmentManager> {
		self.segments.get_mut(segment_num as usize)
	}

	fn free_cache_get(&self) -> Option<u32> {
		self.free_cache.iter().copied().next()
	}

	fn free_cache_store(&mut self, segment_num: u32) {
		self.free_cache.insert(segment_num);
	}

	fn free_cache_evict(&mut self, segment_num: u32) {
		self.free_cache.remove(&segment_num);
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::tempdir;
	use test::Bencher;

	use crate::{
		cache::PageCache,
		consts::PAGE_SIZE_RANGE,
		disk::{
			storage::{self, Storage},
			wal::{self, Wal},
		},
		manage::transaction::TransactionManager,
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = TransactionManager::new(Arc::clone(&cache), wal);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(Arc::clone(&rm)).unwrap();

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[bench]
	#[cfg_attr(miri, ignore)]
	fn bench_alloc_and_free_page(b: &mut Bencher) {
		let page_size = *PAGE_SIZE_RANGE.start();

		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(
			dir.path().join("storage"),
			storage::InitParams { page_size },
		)
		.unwrap();
		Wal::init_file(
			dir.path().join("writes.acnl"),
			wal::InitParams { page_size },
		)
		.unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal = Wal::load_file(
			dir.path().join("writes.acnl"),
			wal::LoadParams { page_size },
		)
		.unwrap();
		let cache = Arc::new(PageCache::new(storage, 64 * 1024));
		let tm = TransactionManager::new(Arc::clone(&cache), wal);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(Arc::clone(&rm)).unwrap();

		b.iter(|| {
			let mut t = tm.begin();
			let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
			t.commit().unwrap();

			let mut t = tm.begin();
			alloc_mgr.free_page(&mut t, page_id).unwrap();
			t.commit().unwrap();
		});
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_free_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = TransactionManager::new(Arc::clone(&cache), wal);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(Arc::clone(&rm)).unwrap();

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
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = TransactionManager::new(Arc::clone(&cache), wal);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(Arc::clone(&rm)).unwrap();

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
