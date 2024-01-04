use std::{
	ops::{Deref, DerefMut},
	sync::Arc,
};

use static_assertions::assert_impl_all;

use crate::{
	cache::{self, PageCache, PageReadGuard, PageWriteGuard},
	index::PageId,
};

use super::{
	api,
	err::Error,
	transaction::{Operation, TransactionManager},
};

pub use super::api::PageRwManager as _;

pub struct PageRwManager<PC = PageCache, TMgr = TransactionManager>
where
	PC: cache::api::PageCache,
	TMgr: api::TransactionManager,
{
	cache: PC,
	transaction_mgr: Arc<TMgr>,
}

assert_impl_all!(PageRwManager: Send, Sync);

impl<PC, TMgr> PageRwManager<PC, TMgr>
where
	PC: cache::api::PageCache,
	TMgr: api::TransactionManager,
{
	pub fn new(cache: PC, transaction_mgr: Arc<TMgr>) -> Self {
		Self {
			cache,
			transaction_mgr,
		}
	}
}

impl<PC, TMgr> api::PageRwManager<TMgr> for PageRwManager<PC, TMgr>
where
	PC: cache::api::PageCache,
	TMgr: api::TransactionManager + 'static,
{
	fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, Error> {
		Ok(self.cache.read_page(page_id)?)
	}

	fn write_page(&self, tid: u64, page_id: PageId) -> Result<PageWriteHandle<TMgr>, Error> {
		self.transaction_mgr.assert_valid_tid(tid)?;
		let page = self.cache.write_page(page_id)?;
		let mut before: Vec<u8> = Vec::with_capacity(page.len());
		page.clone_into(&mut before);

		Ok(PageWriteHandle {
			tid,
			page_id,
			transaction_mgr: &*self.transaction_mgr,
			before: before.into_boxed_slice(),
			guard: page,
		})
	}
}

pub struct PageWriteHandle<'a, TMgr = TransactionManager>
where
	TMgr: api::TransactionManager,
{
	tid: u64,
	page_id: PageId,
	before: Box<[u8]>,
	transaction_mgr: &'a TMgr,
	guard: PageWriteGuard<'a>,
}

impl<'a, TMgr> Drop for PageWriteHandle<'a, TMgr>
where
	TMgr: api::TransactionManager,
{
	fn drop(&mut self) {
		self.transaction_mgr
			.operation(
				self.tid,
				Operation::Write(self.page_id),
				&self.before,
				&self.guard,
			)
			.unwrap();
	}
}

impl<'a, TMgr> Deref for PageWriteHandle<'a, TMgr>
where
	TMgr: api::TransactionManager,
{
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		&self.guard
	}
}

impl<'a, TMgr> DerefMut for PageWriteHandle<'a, TMgr>
where
	TMgr: api::TransactionManager,
{
	#[inline]
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.guard
	}
}

impl<'a, TMgr> AsRef<[u8]> for PageWriteHandle<'a, TMgr>
where
	TMgr: api::TransactionManager,
{
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self
	}
}

impl<'a, TMgr> AsMut<[u8]> for PageWriteHandle<'a, TMgr>
where
	TMgr: api::TransactionManager,
{
	#[inline]
	fn as_mut(&mut self) -> &mut [u8] {
		self
	}
}
