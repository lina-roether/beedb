use std::{
	ops::{Deref, DerefMut},
	sync::Arc,
};

use byte_view::{ByteView, Bytes};
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

	pub fn read_page<T: ?Sized + ByteView>(
		&self,
		page_id: PageId,
	) -> Result<PageReadGuard<T>, Error> {
		Ok(self.cache.read_page(page_id)?)
	}

	pub fn write_page<T: ?Sized + ByteView>(
		&self,
		tid: u64,
		page_id: PageId,
	) -> Result<PageWriteHandle<T>, Error> {
		self.transaction_mgr.assert_valid_tid(tid)?;
		let page = self.cache.write_page::<T>(page_id)?;
		let mut before: Vec<u8> = Vec::with_capacity(page.len());
		page.as_bytes().clone_into(&mut before);

		Ok(PageWriteHandle {
			tid,
			page_id,
			transaction_mgr: &self.transaction_mgr,
			before: before.into_boxed_slice(),
			guard: page,
		})
	}
}

pub struct PageWriteHandle<'a, T: ?Sized + ByteView> {
	tid: u64,
	page_id: PageId,
	before: Box<[u8]>,
	transaction_mgr: &'a TransactionManager,
	guard: PageWriteGuard<'a, T>,
}

impl<'a, T: ?Sized + ByteView> Drop for PageWriteHandle<'a, T> {
	fn drop(&mut self) {
		self.transaction_mgr
			.operation(
				self.tid,
				Operation::Write(self.page_id),
				&self.before,
				self.guard.as_bytes(),
			)
			.unwrap();
	}
}

impl<'a, T: ?Sized + ByteView> Deref for PageWriteHandle<'a, T> {
	type Target = Bytes<T>;

	#[inline]
	fn deref(&self) -> &Self::Target {
		&self.guard
	}
}

impl<'a, T: ?Sized + ByteView> DerefMut for PageWriteHandle<'a, T> {
	#[inline]
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.guard
	}
}

impl<'a, T: ?Sized + ByteView> AsRef<Bytes<T>> for PageWriteHandle<'a, T> {
	#[inline]
	fn as_ref(&self) -> &Bytes<T> {
		self
	}
}

impl<'a, T: ?Sized + ByteView> AsMut<Bytes<T>> for PageWriteHandle<'a, T> {
	#[inline]
	fn as_mut(&mut self) -> &mut Bytes<T> {
		self
	}
}
