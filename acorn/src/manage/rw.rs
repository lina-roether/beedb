use std::sync::Arc;

use static_assertions::assert_impl_all;
use thiserror::Error;

use crate::{
	cache::{PageCache, PageReadGuard, PageWriteGuard},
	disk::{self, DiskStorage},
	index::PageId,
};

use super::transaction::{self, Operation, TransactionManager};

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

pub struct PageRwManager {
	cache: PageCache,
	transaction_mgr: Arc<TransactionManager>,
}

assert_impl_all!(PageRwManager: Send, Sync);

impl PageRwManager {
	pub fn new(
		storage: Arc<DiskStorage>,
		transaction: Arc<TransactionManager>,
		params: Params,
	) -> Self {
		Self {
			cache: PageCache::new(storage, params.cache_size),
			transaction_mgr: transaction,
		}
	}

	pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, disk::Error> {
		self.cache.read_page(page_id)
	}

	pub fn write_page(
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
