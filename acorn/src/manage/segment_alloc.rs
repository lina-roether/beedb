use std::{num::NonZeroU16, sync::Arc};

use byte_view::ViewBuf;
use static_assertions::assert_impl_all;

use crate::{
	id::PageId,
	pages::{FreelistPage, HeaderPage},
};

use super::{
	err::Error,
	transaction::{Transaction, TransactionManager},
};

pub struct SegmentAllocManager {
	segment_num: u32,
	tm: Arc<TransactionManager>,
}

assert_impl_all!(SegmentAllocManager: Send, Sync);

impl SegmentAllocManager {
	const MAX_NUM_PAGES: u16 = u16::MAX;

	pub fn new(tm: Arc<TransactionManager>, segment_num: u32) -> Self {
		Self { segment_num, tm }
	}

	#[inline]
	pub fn segment_num(&self) -> u32 {
		self.segment_num
	}

	pub fn alloc_page(&self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		if let Some(free_page) = self.pop_free_page(t)? {
			return Ok(Some(free_page));
		}
		if let Some(new_page) = self.create_new_page(t)? {
			return Ok(Some(new_page));
		}
		Ok(None)
	}

	pub fn free_page(&self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error> {
		let mut header_page = self.read_header_page()?;

		if let Some(trunk_page_num) = header_page.freelist_trunk {
			let mut trunk_page = self.read_freelist_page(trunk_page_num)?;
			let has_free_space = trunk_page.length < trunk_page.items.len() as u16;

			if has_free_space {
				let index = trunk_page.length as usize;
				trunk_page.items[index] = Some(page_num);
				trunk_page.length += 1;
				self.write_freelist_page(t, trunk_page_num, &trunk_page)?;
				return Ok(());
			}
		};

		let mut new_trunk = self.read_freelist_page(page_num)?;
		new_trunk.next = header_page.freelist_trunk;
		new_trunk.length = 0;
		new_trunk.items.fill(None);
		self.write_freelist_page(t, page_num, &new_trunk)?;

		header_page.freelist_trunk = Some(page_num);
		self.write_header_page(t, &header_page)?;

		Ok(())
	}

	fn create_new_page(&self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		let mut header_page = self.read_header_page()?;
		if header_page.num_pages == Self::MAX_NUM_PAGES {
			return Ok(None);
		}

		let Some(new_page) = NonZeroU16::new(header_page.num_pages) else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};

		header_page.num_pages += 1;
		self.write_header_page(t, &header_page)?;
		Ok(Some(new_page))
	}

	fn pop_free_page(&self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		let mut header_page = self.read_header_page()?;
		let Some(trunk_page_num) = header_page.freelist_trunk else {
			return Ok(None);
		};

		let mut trunk_page = self.read_freelist_page(trunk_page_num)?;

		if trunk_page.length == 0 {
			header_page.freelist_trunk = trunk_page.next;
			self.write_header_page(t, &header_page)?;

			return Ok(Some(trunk_page_num));
		}

		let last_free = trunk_page.length as usize - 1;
		let Some(popped_page) = trunk_page.items[last_free] else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};

		trunk_page.length -= 1;
		trunk_page.items[last_free] = None;
		self.write_freelist_page(t, trunk_page_num, &trunk_page)?;

		Ok(Some(popped_page))
	}

	fn read_header_page(&self) -> Result<ViewBuf<HeaderPage>, Error> {
		let mut buf: ViewBuf<HeaderPage> = ViewBuf::new();
		self.tm.read(self.header_page_id(), buf.as_bytes_mut())?;
		Ok(buf)
	}

	fn write_header_page(
		&self,
		t: &mut Transaction,
		buf: &ViewBuf<HeaderPage>,
	) -> Result<(), Error> {
		t.write(self.header_page_id(), buf.as_bytes())
	}

	fn read_freelist_page(&self, page_num: NonZeroU16) -> Result<ViewBuf<FreelistPage>, Error> {
		let mut buf: ViewBuf<FreelistPage> = ViewBuf::new();
		self.tm
			.read(self.page_id(page_num.get()), buf.as_bytes_mut())?;
		Ok(buf)
	}

	fn write_freelist_page(
		&self,
		t: &mut Transaction,
		page_num: NonZeroU16,
		buf: &ViewBuf<FreelistPage>,
	) -> Result<(), Error> {
		t.write(self.page_id(page_num.get()), buf.as_bytes())
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
		disk::storage::{self, Storage},
		manage::transaction::TransactionManager,
		wal::{self, Wal},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(cache, wal));
		let alloc_mgr = SegmentAllocManager::new(Arc::clone(&tm), 0);

		let mut t = tm.begin();
		let page = alloc_mgr.alloc_page(&mut t).unwrap().unwrap();
		t.commit().unwrap();

		assert_eq!(page, NonZeroU16::new(1).unwrap());
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_and_free_page() {
		let dir = tempdir().unwrap();
		fs::create_dir(dir.path().join("storage")).unwrap();
		Storage::init(dir.path().join("storage"), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().join("storage")).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let tm = Arc::new(TransactionManager::new(cache, wal));
		let alloc_mgr = SegmentAllocManager::new(Arc::clone(&tm), 0);

		let mut t = tm.begin();
		let page = alloc_mgr.alloc_page(&mut t).unwrap().unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		alloc_mgr.free_page(&mut t, page).unwrap();
		t.commit().unwrap();
	}
}
