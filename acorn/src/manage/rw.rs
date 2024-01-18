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

use super::{err::Error, transaction::TransactionManager};

pub struct PageRwManager {
	cache: Arc<PageCache>,
	transaction_mgr: Arc<TransactionManager>,
}

assert_impl_all!(PageRwManager: Send, Sync);

impl PageRwManager {
	pub fn new(cache: Arc<PageCache>, transaction_mgr: Arc<TransactionManager>) -> Self {
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
		let page = self.cache.write_page::<T>(page_id)?;

		Ok(PageWriteHandle {
			tid,
			page_id,
			transaction_mgr: &self.transaction_mgr,
			guard: page,
		})
	}
}

pub struct PageWriteHandle<'a, T: ?Sized + ByteView> {
	tid: u64,
	page_id: PageId,
	transaction_mgr: &'a TransactionManager,
	guard: PageWriteGuard<'a, T>,
}

impl<'a, T: ?Sized + ByteView> Drop for PageWriteHandle<'a, T> {
	fn drop(&mut self) {
		self.transaction_mgr
			.track_write(self.tid, self.page_id, self.guard.as_bytes());
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

#[cfg(test)]
mod tests {
	use std::{fs, mem};

	use tempfile::tempdir;

	use crate::{
		disk::{self, DiskStorage},
		wal::{self, Wal},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn read_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new(wal));
		let rw_mgr = PageRwManager::new(Arc::clone(&cache), transaction_mgr);

		let mut page = cache.write_page::<[u8]>(PageId::new(69, 420)).unwrap();
		page.fill(25);
		mem::drop(page);

		let page = rw_mgr.read_page::<[u8]>(PageId::new(69, 420)).unwrap();
		assert!(page.as_bytes().iter().all(|b| *b == 25));
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn write_page() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new(wal));
		let rw_mgr = PageRwManager::new(Arc::clone(&cache), Arc::clone(&transaction_mgr));

		let tid = transaction_mgr.begin();

		let mut page = rw_mgr
			.write_page::<[u8]>(tid, PageId::new(69, 420))
			.unwrap();
		page.fill(25);
		mem::drop(page);

		transaction_mgr.commit(tid).unwrap();

		let result_page = cache.read_page::<[u8]>(PageId::new(69, 420)).unwrap();
		assert!(result_page.as_bytes().iter().all(|b| *b == 25));
	}
}
