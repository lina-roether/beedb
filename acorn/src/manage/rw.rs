use std::{
	ops::{Deref, DerefMut},
	sync::Arc,
};

use static_assertions::assert_impl_all;

use crate::{
	cache::{PageCache, PageReadGuard, PageWriteGuard},
	index::PageId,
};

use super::{
	err::Error,
	transaction::{Operation, TransactionManager},
};

pub struct PageRwManager {
	cache: PageCache,
	transaction_mgr: Arc<TransactionManager>,
}

assert_impl_all!(PageRwManager: Send, Sync);

impl PageRwManager {
	pub fn new(cache: PageCache, transaction_mgr: Arc<TransactionManager>) -> Self {
		Self {
			cache,
			transaction_mgr,
		}
	}

	pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, Error> {
		Ok(self.cache.read_page(page_id)?)
	}

	pub fn write_page(&self, tid: u64, page_id: PageId) -> Result<PageWriteHandle, Error> {
		self.transaction_mgr.assert_valid_tid(tid)?;
		let page = self.cache.write_page(page_id)?;
		let mut before: Vec<u8> = Vec::with_capacity(page.len());
		page.clone_into(&mut before);

		Ok(PageWriteHandle {
			tid,
			page_id,
			transaction_mgr: &self.transaction_mgr,
			before: before.into_boxed_slice(),
			guard: page,
		})
	}
}

pub struct PageWriteHandle<'a> {
	tid: u64,
	page_id: PageId,
	before: Box<[u8]>,
	transaction_mgr: &'a TransactionManager,
	guard: PageWriteGuard<'a>,
}

impl<'a> Drop for PageWriteHandle<'a> {
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

impl<'a> Deref for PageWriteHandle<'a> {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		&self.guard
	}
}

impl<'a> DerefMut for PageWriteHandle<'a> {
	#[inline]
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.guard
	}
}

impl<'a> AsRef<[u8]> for PageWriteHandle<'a> {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self
	}
}

impl<'a> AsMut<[u8]> for PageWriteHandle<'a> {
	#[inline]
	fn as_mut(&mut self) -> &mut [u8] {
		self
	}
}
