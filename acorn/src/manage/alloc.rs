use std::{collections::HashSet, num::NonZeroU16};

#[cfg(test)]
use mockall::{automock, concretize};

use parking_lot::Mutex;

use crate::{id::PageId, utils::array_map::ArrayMap};

use super::{
	err::Error,
	segment::{SegmentManagerApi as _, SegmentManagerFactory, SegmentManagerFactoryApi},
	transaction::TransactionApi,
};

#[cfg_attr(test, automock)]
pub(super) trait AllocManagerApi {
	#[cfg_attr(test, concretize)]
	fn alloc_page<Transaction>(&self, t: &mut Transaction) -> Result<PageId, Error>
	where
		Transaction: TransactionApi;

	#[cfg_attr(test, concretize)]
	fn free_page<Transaction>(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error>
	where
		Transaction: TransactionApi;
}

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
}

impl<SegmentManagerFactory> AllocManagerApi for AllocManager<SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
	fn free_page<Transaction>(&self, t: &mut Transaction, page_id: PageId) -> Result<(), Error>
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

	fn alloc_page<Transaction>(&self, t: &mut Transaction) -> Result<PageId, Error>
	where
		Transaction: TransactionApi,
	{
		if let Some(page_id) = self.alloc_from_free_cache(t)? {
			return Ok(page_id);
		}
		self.alloc_in_new_segment(t)
	}
}

impl<SegmentManagerFactory> AllocManager<SegmentManagerFactory>
where
	SegmentManagerFactory: SegmentManagerFactoryApi,
{
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

	use mockall::predicate::*;
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
			segment::{MockSegmentManagerApi, MockSegmentManagerFactoryApi},
			transaction::{MockTransactionApi, TransactionManager, TransactionManagerApi as _},
		},
	};

	use super::*;

	#[test]
	fn alloc_page_simple() {
		// expect
		let mut segment_factory = MockSegmentManagerFactoryApi::new();
		segment_factory
			.expect_build_existing()
			.returning(|| vec![].into_iter());
		segment_factory.expect_build().with(eq(0)).returning(|_| {
			let mut segment = MockSegmentManagerApi::new();
			segment.expect_has_free_pages().returning(|| true);
			segment.expect_segment_num().returning(|| 0);
			segment
				.expect_alloc_page()
				.returning(|_| Ok(NonZeroU16::new(1)));
			Ok(segment)
		});

		// given
		let alloc_mgr = AllocManager::new(segment_factory).unwrap();

		// when
		let page_id = alloc_mgr
			.alloc_page(&mut MockTransactionApi::new())
			.unwrap();

		// then
		assert_eq!(page_id, PageId::new(0, 1));
	}

	#[test]
	fn alloc_page_in_new_segment() {
		// expect
		let mut segment_factory = MockSegmentManagerFactoryApi::new();
		segment_factory.expect_build_existing().returning(|| {
			let mut segment_0 = MockSegmentManagerApi::new();
			segment_0.expect_segment_num().returning(|| 0);
			segment_0.expect_has_free_pages().returning(|| false);

			let mut segment_1 = MockSegmentManagerApi::new();
			segment_1.expect_segment_num().returning(|| 1);
			segment_1.expect_has_free_pages().returning(|| false);

			vec![Ok(segment_0), Ok(segment_1)].into_iter()
		});
		segment_factory.expect_build().with(eq(2)).returning(|_| {
			let mut segment = MockSegmentManagerApi::new();
			segment.expect_segment_num().returning(|| 2);
			segment.expect_has_free_pages().returning(|| true);
			segment
				.expect_alloc_page()
				.returning(|_| Ok(NonZeroU16::new(4)));
			Ok(segment)
		});

		// given
		let alloc_mgr = AllocManager::new(segment_factory).unwrap();

		// when
		let page_id = alloc_mgr
			.alloc_page(&mut MockTransactionApi::new())
			.unwrap();

		// then
		assert_eq!(page_id, PageId::new(2, 4))
	}

	#[test]
	fn alloc_page_in_existing_segment() {
		// expect
		let mut segment_factory = MockSegmentManagerFactoryApi::new();
		segment_factory.expect_build_existing().returning(|| {
			let mut segment_0 = MockSegmentManagerApi::new();
			segment_0.expect_segment_num().returning(|| 0);
			segment_0.expect_has_free_pages().returning(|| false);

			let mut segment_1 = MockSegmentManagerApi::new();
			segment_1.expect_segment_num().returning(|| 1);
			segment_1.expect_has_free_pages().returning(|| true);
			segment_1
				.expect_alloc_page()
				.returning(|_| Ok(NonZeroU16::new(69)));

			vec![Ok(segment_0), Ok(segment_1)].into_iter()
		});

		// given
		let alloc_mgr = AllocManager::new(segment_factory).unwrap();

		// when
		let page_id = alloc_mgr
			.alloc_page(&mut MockTransactionApi::new())
			.unwrap();

		// then
		assert_eq!(page_id, PageId::new(1, 69))
	}

	#[test]
	fn free_page() {
		// expect
		let mut segment_factory = MockSegmentManagerFactoryApi::new();
		segment_factory.expect_build_existing().returning(|| {
			let mut segment_0 = MockSegmentManagerApi::new();
			segment_0.expect_segment_num().returning(|| 0);
			segment_0.expect_has_free_pages().returning(|| false);
			segment_0
				.expect_free_page()
				.withf(|_, page_num| page_num.get() == 25)
				.returning(|_, _| Ok(()));
			vec![Ok(segment_0)].into_iter()
		});

		// given
		let alloc_mgr = AllocManager::new(segment_factory).unwrap();

		// when
		alloc_mgr
			.free_page(&mut MockTransactionApi::new(), PageId::new(0, 25))
			.unwrap();
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
}
