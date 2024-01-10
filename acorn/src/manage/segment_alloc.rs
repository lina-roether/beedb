use std::{mem, num::NonZeroU16, sync::Arc};

use parking_lot::{lock_api::RawMutex as _, RawMutex};
use static_assertions::assert_impl_all;

use crate::{
	cache::PageReadGuard,
	index::PageId,
	pages::{FreelistPage, HeaderPage},
};

use super::{
	err::Error,
	rw::{PageRwManager, PageWriteHandle},
};

pub struct SegmentAllocManager {
	segment_num: u32,
	rw_mgr: Arc<PageRwManager>,
	alloc_lock: RawMutex,
}

assert_impl_all!(SegmentAllocManager: Send, Sync);

impl SegmentAllocManager {
	const MAX_NUM_PAGES: u16 = u16::MAX;

	pub fn new(rw_mgr: Arc<PageRwManager>, segment_num: u32) -> Self {
		Self {
			segment_num,
			rw_mgr,
			alloc_lock: RawMutex::INIT,
		}
	}

	#[inline]
	pub fn segment_num(&self) -> u32 {
		self.segment_num
	}

	pub fn alloc_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		if let Some(free_page) = self.pop_free_page(tid)? {
			return Ok(Some(free_page));
		}
		if let Some(new_page) = self.create_new_page(tid)? {
			return Ok(Some(new_page));
		}
		Ok(None)
	}

	pub fn free_page(&self, tid: u64, page_num: NonZeroU16) -> Result<(), Error> {
		self.alloc_lock.lock();

		let trunk_page_num = self.freelist_trunk()?;

		if let Some(trunk_page_num) = trunk_page_num {
			let trunk_page = self.read_freelist_page(trunk_page_num)?;
			let has_free_space = trunk_page.length < trunk_page.items.len() as u16;
			mem::drop(trunk_page);

			if has_free_space {
				let mut trunk_page = self.write_freelist_page(tid, trunk_page_num)?;
				let index = trunk_page.length as usize;
				trunk_page.items[index] = Some(page_num);
				trunk_page.length += 1;
			}
		};

		let mut new_trunk = self.write_freelist_page(tid, page_num)?;
		new_trunk.next = trunk_page_num;
		new_trunk.length = 0;
		new_trunk.items.fill(None);
		mem::drop(new_trunk);

		self.set_freelist_trunk(tid, Some(page_num))?;

		unsafe { self.alloc_lock.unlock() }
		Ok(())
	}

	fn create_new_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		self.alloc_lock.lock();

		let header_page = self.read_header_page()?;
		if header_page.num_pages == Self::MAX_NUM_PAGES {
			return Ok(None);
		}
		let Some(new_page) = NonZeroU16::new(header_page.num_pages) else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		mem::drop(header_page);

		let mut header_page = self.write_header_page(tid)?;
		header_page.num_pages += 1;
		mem::drop(header_page);

		unsafe { self.alloc_lock.unlock() }
		Ok(Some(new_page))
	}

	fn pop_free_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		self.alloc_lock.lock();

		let Some(trunk_page_num) = self.freelist_trunk()? else {
			return Ok(None);
		};

		let trunk_page = self.read_freelist_page(trunk_page_num)?;

		if trunk_page.length == 0 {
			let new_trunk = trunk_page.next;
			mem::drop(trunk_page);
			self.set_freelist_trunk(tid, new_trunk)?;
			return Ok(Some(trunk_page_num));
		}

		let last_free = trunk_page.length as usize - 1;
		let Some(popped_page) = trunk_page.items[last_free] else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		mem::drop(trunk_page);

		let mut trunk_page = self.write_freelist_page(tid, trunk_page_num)?;
		trunk_page.length -= 1;
		trunk_page.items[last_free] = None;
		mem::drop(trunk_page);

		unsafe { self.alloc_lock.unlock() }
		Ok(Some(popped_page))
	}

	fn set_freelist_trunk(&self, tid: u64, trunk: Option<NonZeroU16>) -> Result<(), Error> {
		self.write_header_page(tid)?.freelist_trunk = trunk;
		Ok(())
	}

	fn freelist_trunk(&self) -> Result<Option<NonZeroU16>, Error> {
		Ok(self.read_header_page()?.freelist_trunk)
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
