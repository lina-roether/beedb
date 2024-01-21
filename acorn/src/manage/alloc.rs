use std::{num::NonZeroU16, sync::Arc};

use parking_lot::Mutex;

use crate::{id::PageId, utils::array_map::ArrayMap};

use super::{
	err::Error,
	segment_alloc::SegmentAllocManager,
	transaction::{Transaction, TransactionManager},
};

pub struct AllocManager {
	state: Mutex<State>,
	tm: Arc<TransactionManager>,
}

impl AllocManager {
	pub fn new(tm: Arc<TransactionManager>) -> Result<Self, Error> {
		let mut state = State {
			segments: ArrayMap::new(),
			free_stack: Vec::new(),
			next_segment: 0,
		};
		for segment_num in tm.segment_nums().iter() {
			state.add_segment(*segment_num, Arc::clone(&tm))?;
		}

		Ok(Self {
			state: Mutex::new(state),
			tm,
		})
	}

	pub fn free_page(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error> {
		let state = self.state.lock();

		let Some(page_num) = NonZeroU16::new(page_id.page_num) else {
			return Ok(());
		};

		let Some(segment) = state.get_segment(page_id.segment_num) else {
			return Ok(());
		};
		segment.free_page(t, page_num)
	}

	pub fn alloc_page(&self, t: &mut Transaction) -> Result<PageId, Error> {
		if let Some(page_id) = self.alloc_from_free_stack(t)? {
			return Ok(page_id);
		}
		self.alloc_in_new_segment(t)
	}

	fn alloc_in_new_segment(&self, t: &mut Transaction) -> Result<PageId, Error> {
		let mut state = self.state.lock();
		let next_segment = state.next_segment;
		let Some(page_id) = state.try_alloc_in_new(t, next_segment, Arc::clone(&self.tm))? else {
			return Err(Error::SizeLimitReached);
		};
		Ok(page_id)
	}

	fn alloc_from_free_stack(&self, t: &mut Transaction) -> Result<Option<PageId>, Error> {
		let mut state = self.state.lock();
		loop {
			let Some(segment_num) = state.free_stack.last() else {
				return Ok(None);
			};
			let Some(page_id) = state.try_alloc_in_existing(t, *segment_num)? else {
				state.free_stack.pop();
				continue;
			};
			return Ok(Some(page_id));
		}
	}
}

struct State {
	segments: ArrayMap<SegmentAllocManager>,
	free_stack: Vec<u32>,
	next_segment: u32,
}

impl State {
	fn try_alloc_in(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
		tm: Arc<TransactionManager>,
	) -> Result<Option<PageId>, Error> {
		if self.has_segment(segment_num) {
			self.try_alloc_in_existing(t, segment_num)
		} else {
			self.try_alloc_in_new(t, segment_num, tm)
		}
	}

	fn try_alloc_in_existing(
		&self,
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
		tm: Arc<TransactionManager>,
	) -> Result<Option<PageId>, Error> {
		let segment = self.add_segment(segment_num, tm)?;
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
		tm: Arc<TransactionManager>,
	) -> Result<&SegmentAllocManager, Error> {
		let segment_alloc = SegmentAllocManager::new(tm, segment_num);
		if segment_alloc.has_free_pages()? {
			self.free_stack.push(segment_num);
		}
		if segment_num >= self.next_segment {
			self.next_segment = segment_num + 1;
		}
		self.segments.insert(segment_num as usize, segment_alloc);
		Ok(self.segments.get(segment_num as usize).unwrap())
	}

	fn get_segment(&self, segment_num: u32) -> Option<&SegmentAllocManager> {
		self.segments.get(segment_num as usize)
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
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm)).unwrap();

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[bench]
	#[cfg_attr(miri, ignore)]
	fn bench_alloc_page(b: &mut Bencher) {
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
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm)).unwrap();

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
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm)).unwrap();

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
		let tm = Arc::new(TransactionManager::new(Arc::clone(&cache), wal));
		let alloc_mgr = AllocManager::new(Arc::clone(&tm)).unwrap();

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
