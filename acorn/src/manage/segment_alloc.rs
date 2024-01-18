use std::{mem, num::NonZeroU16, sync::Arc};

use static_assertions::assert_impl_all;

use crate::{
	cache::PageReadGuard,
	id::PageId,
	pages::{FreelistPage, HeaderPage},
};

use super::{
	err::Error,
	rw::{PageRwManager, PageWriteHandle},
};

pub struct SegmentAllocManager {
	segment_num: u32,
	rw_mgr: Arc<PageRwManager>,
}

assert_impl_all!(SegmentAllocManager: Send, Sync);

impl SegmentAllocManager {
	const MAX_NUM_PAGES: u16 = u16::MAX;

	pub fn new(rw_mgr: Arc<PageRwManager>, segment_num: u32) -> Self {
		Self {
			segment_num,
			rw_mgr,
		}
	}

	#[inline]
	pub fn segment_num(&self) -> u32 {
		self.segment_num
	}

	pub fn alloc_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		let mut header_page = self.write_header_page(tid)?;
		if let Some(free_page) = self.pop_free_page(tid, &mut header_page)? {
			return Ok(Some(free_page));
		}
		if let Some(new_page) = self.create_new_page(&mut header_page)? {
			return Ok(Some(new_page));
		}
		Ok(None)
	}

	pub fn free_page(&self, tid: u64, page_num: NonZeroU16) -> Result<(), Error> {
		let mut header_page = self.write_header_page(tid)?;

		if let Some(trunk_page_num) = header_page.freelist_trunk {
			let trunk_page = self.read_freelist_page(trunk_page_num)?;
			let has_free_space = trunk_page.length < trunk_page.items.len() as u16;
			mem::drop(trunk_page);

			if has_free_space {
				let mut trunk_page = self.write_freelist_page(tid, trunk_page_num)?;
				let index = trunk_page.length as usize;
				trunk_page.items[index] = Some(page_num);
				trunk_page.length += 1;
				return Ok(());
			}
		};

		let mut new_trunk = self.write_freelist_page(tid, page_num)?;
		new_trunk.next = header_page.freelist_trunk;
		new_trunk.length = 0;
		new_trunk.items.fill(None);

		header_page.freelist_trunk = Some(page_num);

		Ok(())
	}

	fn create_new_page(
		&self,
		header_page: &mut PageWriteHandle<HeaderPage>,
	) -> Result<Option<NonZeroU16>, Error> {
		if header_page.num_pages == Self::MAX_NUM_PAGES {
			return Ok(None);
		}

		let Some(new_page) = NonZeroU16::new(header_page.num_pages) else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};

		header_page.num_pages += 1;
		Ok(Some(new_page))
	}

	fn pop_free_page(
		&self,
		tid: u64,
		header_page: &mut PageWriteHandle<HeaderPage>,
	) -> Result<Option<NonZeroU16>, Error> {
		let Some(trunk_page_num) = header_page.freelist_trunk else {
			return Ok(None);
		};

		let mut trunk_page = self.write_freelist_page(tid, trunk_page_num)?;

		if trunk_page.length == 0 {
			let new_trunk = trunk_page.next;
			header_page.freelist_trunk = new_trunk;
			return Ok(Some(trunk_page_num));
		}

		let last_free = trunk_page.length as usize - 1;
		let Some(popped_page) = trunk_page.items[last_free] else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};

		trunk_page.length -= 1;
		trunk_page.items[last_free] = None;

		Ok(Some(popped_page))
	}

	fn read_header_page(&self) -> Result<PageReadGuard<HeaderPage>, Error> {
		self.rw_mgr.read_page(self.header_page_id())
	}

	fn write_header_page(&self, tid: u64) -> Result<PageWriteHandle<HeaderPage>, Error> {
		self.rw_mgr.write_page(tid, self.header_page_id())
	}

	fn read_freelist_page(
		&self,
		page_num: NonZeroU16,
	) -> Result<PageReadGuard<FreelistPage>, Error> {
		self.rw_mgr.read_page(self.page_id(page_num.get()))
	}

	fn write_freelist_page(
		&self,
		tid: u64,
		page_num: NonZeroU16,
	) -> Result<PageWriteHandle<FreelistPage>, Error> {
		self.rw_mgr.write_page(tid, self.page_id(page_num.get()))
	}

	#[inline]
	fn header_page_id(&self) -> PageId {
		self.page_id(0)
	}

	#[inline]
	fn page_id(&self, page_num: u16) -> PageId {
		PageId::new(self.segment_num, page_num)
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::tempdir;

	use crate::{
		cache::PageCache,
		disk::{self, DiskStorage},
		manage::transaction::TransactionManager,
		wal::{self, Wal},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new(wal));
		let rw_mgr = Arc::new(PageRwManager::new(
			Arc::clone(&cache),
			Arc::clone(&transaction_mgr),
		));
		let alloc_mgr = SegmentAllocManager::new(Arc::clone(&rw_mgr), 0);

		let tid = transaction_mgr.begin();
		let page = alloc_mgr.alloc_page(tid).unwrap().unwrap();
		transaction_mgr.commit(tid).unwrap();

		assert_eq!(page, NonZeroU16::new(1).unwrap());
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_and_free_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		DiskStorage::init(dir.path().join("storage"), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let transaction_mgr = Arc::new(TransactionManager::new(wal));
		let rw_mgr = Arc::new(PageRwManager::new(
			Arc::clone(&cache),
			Arc::clone(&transaction_mgr),
		));
		let alloc_mgr = SegmentAllocManager::new(Arc::clone(&rw_mgr), 0);

		let tid = transaction_mgr.begin();
		let page = alloc_mgr.alloc_page(tid).unwrap().unwrap();
		transaction_mgr.commit(tid).unwrap();

		let tid = transaction_mgr.begin();
		alloc_mgr.free_page(tid, page).unwrap();
		transaction_mgr.commit(tid).unwrap();
	}
}
