use std::{collections::HashSet, num::NonZeroU16};

use parking_lot::Mutex;

use crate::{id::PageId, utils::array_map::ArrayMap};

use super::{
	err::Error,
	segment::{SegmentManagerApi as _, SegmentManagerFactory, SegmentManagerFactoryApi},
	transaction::TransactionApi,
};

pub(super) struct AllocManager<SegmentManagerFactory = self::SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
	state: Mutex<State<SegmentManagerFactory>>,
}

impl<SegmentManagerFactory> AllocManager<SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
	pub fn new(factory: SegmentManagerFactory) -> Result<Self, Error> {
		let mut state = State {
			factory,
			segments: ArrayMap::new(),
			free_cache: HashSet::new(),
			next_segment: 0,
		};
		state.add_existing_segments()?;

		Ok(Self {
			state: Mutex::new(state),
		})
	}

	pub fn free_page<Transaction>(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
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

	pub fn alloc_page<Transaction>(&self, t: &mut Transaction) -> Result<PageId, Error>
	where
		Transaction: TransactionApi,
	{
		if let Some(page_id) = self.alloc_from_free_cache(t)? {
			return Ok(page_id);
		}
		self.alloc_in_new_segment(t)
	}

	fn alloc_in_new_segment<Transaction>(&self, t: &mut Transaction) -> Result<PageId, Error>
	where
		Transaction: TransactionApi,
	{
		let mut state = self.state.lock();
		let next_segment = state.next_segment;
		let Some(page_id) = state.try_alloc_in_new(t, next_segment)? else {
			return Err(Error::SizeLimitReached);
		};
		Ok(page_id)
	}

	fn alloc_from_free_cache<Transaction>(
		&self,
		t: &mut Transaction,
	) -> Result<Option<PageId>, Error>
	where
		Transaction: TransactionApi,
	{
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

struct State<SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
	factory: SegmentManagerFactory,
	segments: ArrayMap<SegmentManagerFactory::SegmentManager>,
	free_cache: HashSet<u32>,
	next_segment: u32,
}

impl<SegmentManagerFactory> State<SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
	fn add_existing_segments(&mut self) -> Result<(), Error> {
		for segment in self.factory.build_existing() {
			self.add_segment(segment?);
		}
		Ok(())
	}

	fn try_alloc_in<Transaction>(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
	) -> Result<Option<PageId>, Error>
	where
		Transaction: TransactionApi,
	{
		if self.has_segment(segment_num) {
			self.try_alloc_in_existing(t, segment_num)
		} else {
			self.try_alloc_in_new(t, segment_num)
		}
	}

	fn try_alloc_in_existing<Transaction>(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
	) -> Result<Option<PageId>, Error>
	where
		Transaction: TransactionApi,
	{
		let Some(segment) = self.get_segment(segment_num) else {
			return Ok(None);
		};
		let Some(page_num) = segment.alloc_page(t)? else {
			return Ok(None);
		};
		Ok(Some(PageId::new(segment_num, page_num.get())))
	}

	fn try_alloc_in_new<Transaction>(
		&mut self,
		t: &mut Transaction,
		segment_num: u32,
	) -> Result<Option<PageId>, Error>
	where
		Transaction: TransactionApi,
	{
		let segment = self.add_segment(self.factory.build(segment_num)?);
		let Some(page_num) = segment.alloc_page(t)? else {
			return Ok(None);
		};
		Ok(Some(PageId::new(segment.segment_num(), page_num.get())))
	}

	fn has_segment(&self, segment_num: u32) -> bool {
		self.segments.has(segment_num as usize)
	}

	fn add_segment(
		&mut self,
		segment_alloc: SegmentManagerFactory::SegmentManager,
	) -> &mut SegmentManagerFactory::SegmentManager {
		let segment_num = segment_alloc.segment_num();
		if segment_alloc.has_free_pages() {
			self.free_cache.insert(segment_num);
		}
		if segment_num >= self.next_segment {
			self.next_segment = segment_num + 1;
		}
		self.segments.insert(segment_num as usize, segment_alloc);
		self.segments.get_mut(segment_num as usize).unwrap()
	}

	fn get_segment(
		&mut self,
		segment_num: u32,
	) -> Option<&mut SegmentManagerFactory::SegmentManager> {
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
	use std::{fs, sync::Arc};

	use tempfile::tempdir;
	use test::Bencher;

	use crate::{
		cache::PageCache,
		consts::DEFAULT_PAGE_SIZE,
		disk::{
			storage::{self, Storage},
			wal::Wal,
		},
		manage::{
			read::ReadManager,
			recovery::RecoveryManager,
			transaction::{TransactionManager, TransactionManagerApi as _},
		},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(SegmentManagerFactory::new(rm)).unwrap();

		let mut t = tm.begin();
		let page_id = alloc_mgr.alloc_page(&mut t).unwrap();
		t.commit().unwrap();

		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[bench]
	#[cfg_attr(miri, ignore)]
	fn bench_alloc_and_free_page(b: &mut Bencher) {
		let page_size = DEFAULT_PAGE_SIZE;

		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(
			dir.path().join("storage"),
			storage::InitParams { page_size },
		)
		.unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(SegmentManagerFactory::new(rm)).unwrap();

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
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(SegmentManagerFactory::new(rm)).unwrap();

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
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let alloc_mgr = AllocManager::new(SegmentManagerFactory::new(rm)).unwrap();

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
