use std::sync::Arc;

use static_assertions::assert_impl_all;
use thiserror::Error;

use crate::{
	cache::{PageCache, PageReadGuard, PageWriteGuard},
	disk::{self, DiskStorage},
	index::PageId,
};

use super::{
	api,
	transaction::{self, Operation, TransactionManager},
};

pub use super::api::PageRwManager as _;

pub struct Params {
	pub cache_size: usize,
}

#[derive(Debug, Error)]
pub enum WriteError {
	#[error(transparent)]
	Disk(#[from] disk::Error),

	#[error(transparent)]
	Transaction(#[from] transaction::Error),
}

pub struct PageRwManager<TMgr = TransactionManager>
where
	TMgr: api::TransactionManager,
{
	cache: PageCache,
	transaction_mgr: Arc<TMgr>,
}

assert_impl_all!(PageRwManager: Send, Sync);

impl<TMgr> PageRwManager<TMgr>
where
	TMgr: api::TransactionManager,
{
	pub fn new(storage: Arc<DiskStorage>, transaction_mgr: Arc<TMgr>, params: Params) -> Self {
		Self {
			cache: PageCache::new(storage, params.cache_size),
			transaction_mgr,
		}
	}
}

impl<TMgr> api::PageRwManager for PageRwManager<TMgr>
where
	TMgr: api::TransactionManager,
{
	fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, disk::Error> {
		self.cache.read_page(page_id)
	}

	fn write_page(
		&self,
		tid: u64,
		page_id: PageId,
		write_fn: impl FnOnce(&mut PageWriteGuard),
	) -> Result<(), WriteError> {
		let mut page = self.cache.write_page(page_id)?;
		let mut before: Vec<u8> = Vec::new();
		page.clone_into(&mut before);

		write_fn(&mut page);

		self.transaction_mgr
			.operation(tid, Operation::Write(page_id), &before, page.as_ref())?;
		Ok(())
	}
}
