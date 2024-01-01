use std::sync::Arc;

use static_assertions::assert_impl_all;

use crate::{
	cache::{PageCache, PageReadGuard},
	disk::{self, DiskStorage},
	index::PageId,
};

use super::transaction::{self, TransactionManager};

/*
 * TODO: WriteManager should also be in charge of functionality like WAL, as
 * soon as that's implemeneted.
 */

pub struct Params {
	pub cache_size: usize,
}

pub struct PageAccessManager {
	cache: PageCache,
	transaction: TransactionManager,
}

assert_impl_all!(PageAccessManager: Send, Sync);

impl PageAccessManager {
	pub fn new(storage: Arc<DiskStorage>, params: Params) -> Self {
		Self {
			cache: PageCache::new(storage, params.cache_size),
			transaction: TransactionManager::new(),
		}
	}

	pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, disk::Error> {
		self.cache.read_page(page_id)
	}

	pub fn begin(&self) -> u64 {
		self.transaction.begin()
	}

	pub fn write_page(&self, tid: u64, page_id: PageId) {
		todo!()
	}

	pub fn commit(&self, tid: u64) -> Result<(), transaction::Error> {
		self.transaction.commit(tid)
	}
}
