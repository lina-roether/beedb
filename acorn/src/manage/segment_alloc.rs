use std::{marker::PhantomData, mem, num::NonZeroU16, sync::Arc};

use parking_lot::{lock_api::RawMutex as _, RawMutex};
use static_assertions::assert_impl_all;

use crate::{
	index::PageId,
	pages::{FreelistPage, FreelistPageHeader, HeaderPage},
	utils::byte_view::ByteView,
};

use super::{
	api::{self},
	err::Error,
	rw::PageRwManager,
	transaction::TransactionManager,
};

pub use super::api::SegmentAllocManager as _;

pub struct SegmentAllocManager<TMgr = TransactionManager, RwMgr = PageRwManager>
where
	TMgr: api::TransactionManager + 'static,
	RwMgr: api::PageRwManager<TMgr>,
{
	segment_num: u32,
	rw_mgr: Arc<RwMgr>,
	alloc_lock: RawMutex,
	_marker: PhantomData<TMgr>,
}

assert_impl_all!(SegmentAllocManager: Send, Sync);

impl<TMgr, RwMgr> SegmentAllocManager<TMgr, RwMgr>
where
	TMgr: api::TransactionManager,
	RwMgr: api::PageRwManager<TMgr>,
{
	const MAX_NUM_PAGES: u16 = u16::MAX;

	pub fn new(rw_mgr: Arc<RwMgr>, segment_num: u32) -> Self {
		Self {
			segment_num,
			rw_mgr,
			alloc_lock: RawMutex::INIT,
			_marker: PhantomData,
		}
	}

	fn create_new_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		self.alloc_lock.lock();

		let header_page_bytes = self.rw_mgr.read_page(self.header_page_id())?;
		let header_page = HeaderPage::from_bytes(&header_page_bytes);

		if header_page.num_pages == Self::MAX_NUM_PAGES {
			return Ok(None);
		}

		let Some(new_page) = NonZeroU16::new(header_page.num_pages) else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		mem::drop(header_page_bytes);

		let mut header_page_bytes = self.rw_mgr.write_page(tid, self.header_page_id())?;
		let header_page = HeaderPage::from_bytes_mut(&mut header_page_bytes);
		header_page.num_pages += 1;
		mem::drop(header_page_bytes);

		unsafe { self.alloc_lock.unlock() }
		Ok(Some(new_page))
	}

	fn pop_free_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error> {
		self.alloc_lock.lock();

		let Some(trunk_page_num) = self.freelist_trunk()? else {
			return Ok(None);
		};

		let trunk_page_bytes = self.rw_mgr.read_page(self.page_id(trunk_page_num.get()))?;
		let trunk_page = FreelistPage::from_bytes(&trunk_page_bytes);

		if trunk_page.header.length == 0 {
			let new_trunk = trunk_page.header.next;
			mem::drop(trunk_page_bytes);
			self.set_freelist_trunk(tid, new_trunk)?;
			return Ok(Some(trunk_page_num));
		}

		let last_free = trunk_page.header.length as usize - 1;
		let Some(popped_page) = trunk_page.items[last_free] else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		mem::drop(trunk_page_bytes);

		let mut trunk_page_bytes_mut = self
			.rw_mgr
			.write_page(tid, self.page_id(trunk_page_num.get()))?;

		let trunk_page = FreelistPage::from_bytes_mut(&mut trunk_page_bytes_mut);
		trunk_page.header.length -= 1;
		trunk_page.items[last_free] = None;
		mem::drop(trunk_page_bytes_mut);

		unsafe { self.alloc_lock.unlock() }
		Ok(Some(popped_page))
	}

	fn set_freelist_trunk(&self, tid: u64, trunk: Option<NonZeroU16>) -> Result<(), Error> {
		let mut trunk_page_bytes = self.rw_mgr.write_page(tid, self.header_page_id())?;
		HeaderPage::from_bytes_mut(&mut trunk_page_bytes).freelist_trunk = trunk;
		Ok(())
	}

	fn freelist_trunk(&self) -> Result<Option<NonZeroU16>, Error> {
		let header_page = self.rw_mgr.read_page(self.header_page_id())?;
		Ok(HeaderPage::from_bytes(&header_page).freelist_trunk)
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

impl<TMgr, RwMgr> SegmentAllocManager<TMgr, RwMgr>
where
	TMgr: api::TransactionManager,
	RwMgr: api::PageRwManager<TMgr>,
{
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
			let trunk_page_bytes = self.rw_mgr.read_page(self.page_id(trunk_page_num.get()))?;
			let trunk_page = FreelistPage::from_bytes(&trunk_page_bytes);
			let has_free_space = trunk_page.header.length < trunk_page.items.len() as u16;
			mem::drop(trunk_page_bytes);
			if has_free_space {
				let mut trunk_page_bytes_mut = self
					.rw_mgr
					.write_page(tid, self.page_id(trunk_page_num.get()))?;

				let trunk_page = FreelistPage::from_bytes_mut(&mut trunk_page_bytes_mut);
				trunk_page.items[trunk_page.header.length as usize] = Some(page_num);
				trunk_page.header.length += 1;
			}
		};

		let mut new_trunk_bytes = self.rw_mgr.write_page(tid, self.page_id(page_num.get()))?;
		let new_trunk = FreelistPage::from_bytes_mut(&mut new_trunk_bytes);
		new_trunk.header = FreelistPageHeader {
			next: trunk_page_num,
			length: 0,
		};
		new_trunk.items.fill(None);
		mem::drop(new_trunk_bytes);

		self.set_freelist_trunk(tid, Some(page_num))?;

		unsafe { self.alloc_lock.unlock() }
		Ok(())
	}
}
