use std::{num::NonZeroU16, sync::Arc};

use byte_view::{BufError, ViewBuf};

use crate::{
	id::PageId,
	pages::{FreelistPage, HeaderPage, WriteOp},
};

use super::{err::Error, read::ReadManager, transaction::Transaction};

pub(super) struct SegmentManager {
	segment_num: u32,
	rm: Arc<ReadManager>,
	header: ViewBuf<HeaderPage>,
	freelist_stack: Vec<FreelistStackEntry>,
}

impl SegmentManager {
	pub fn new(rm: Arc<ReadManager>, segment_num: u32) -> Result<Self, Error> {
		let mut header: ViewBuf<HeaderPage> = ViewBuf::new();
		rm.read(PageId::new(segment_num, 0), HeaderPage::read(&mut header))?;

		let mut freelist_stack = Vec::new();
		let mut next = header.freelist_trunk;
		while let Some(page_num) = next {
			let mut entry = FreelistStackEntry::new(page_num, rm.page_size()).unwrap();
			rm.read(
				PageId::new(segment_num, page_num.get()),
				FreelistPage::read(&mut entry.buf),
			)?;
			next = entry.buf.next;
			freelist_stack.push(entry);
		}
		freelist_stack.reverse();

		Ok(Self {
			segment_num,
			rm,
			header,
			freelist_stack,
		})
	}

	pub fn alloc_page(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		if let Some(free_page) = self.pop_freelist(t)? {
			return Ok(Some(free_page));
		}
		if let Some(new_page) = self.create_new_page(t)? {
			return Ok(Some(new_page));
		}
		Ok(None)
	}

	pub fn free_page(&mut self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error> {
		self.push_freelist(t, page_num)
	}

	pub fn has_free_pages(&self) -> bool {
		!self.freelist_stack.is_empty()
	}

	fn create_new_page(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		if self.header.num_pages == u16::MAX {
			return Ok(None);
		}

		let Some(new_page) = NonZeroU16::new(self.header.num_pages) else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		self.header.num_pages += 1;
		self.write_header(t)?;

		Ok(Some(new_page))
	}

	fn push_freelist(&mut self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error> {
		if let Some(mut trunk) = self.get_trunk() {
			if !trunk.is_full() {
				trunk.push(t, page_num)?;
				return Ok(());
			}

			trunk.set_next(t, Some(page_num))?;
		}

		self.push_trunk(t, page_num)?;
		Ok(())
	}

	fn pop_freelist(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		let Some(mut trunk) = self.get_trunk() else {
			return Ok(None);
		};

		let next = trunk.next();
		let page_num = trunk.page_num();

		let Some(page_num) = trunk.pop(t)? else {
			self.set_trunk(t, next)?;
			self.freelist_stack.pop();
			return Ok(Some(page_num));
		};

		Ok(Some(page_num))
	}

	fn get_trunk(&mut self) -> Option<FreelistPageManager> {
		self.freelist_stack
			.last_mut()
			.map(|page| FreelistPageManager::new(self.segment_num, page))
	}

	fn push_trunk(&mut self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error> {
		self.set_trunk(t, Some(page_num))?;
		self.freelist_stack
			.push(FreelistStackEntry::new(page_num, self.rm.page_size()).unwrap());
		let mut new_trunk = self.get_trunk().unwrap();
		new_trunk.reset(t)?;
		Ok(())
	}

	fn set_trunk(
		&mut self,
		t: &mut Transaction,
		trunk_num: Option<NonZeroU16>,
	) -> Result<(), Error> {
		self.header.freelist_trunk = trunk_num;
		self.write_header(t)?;
		Ok(())
	}

	fn write_header(&self, t: &mut Transaction) -> Result<(), Error> {
		self.write(t, 0, HeaderPage::write(&self.header))
	}

	fn write(&self, t: &mut Transaction, page_num: u16, op: WriteOp) -> Result<(), Error> {
		t.write(PageId::new(self.segment_num, page_num), op)
	}
}

struct FreelistStackEntry {
	page_num: NonZeroU16,
	buf: ViewBuf<FreelistPage>,
}

impl FreelistStackEntry {
	fn new(page_num: NonZeroU16, page_size: u16) -> Result<Self, BufError> {
		let buf: ViewBuf<FreelistPage> = ViewBuf::new_with_size(page_size.into())?;
		Ok(Self { page_num, buf })
	}
}

struct FreelistPageManager<'a> {
	segment_num: u32,
	page: &'a mut FreelistStackEntry,
}

impl<'a> FreelistPageManager<'a> {
	fn new(segment_num: u32, page: &'a mut FreelistStackEntry) -> Self {
		Self { segment_num, page }
	}

	#[inline]
	fn buf(&self) -> &ViewBuf<FreelistPage> {
		&self.page.buf
	}

	#[inline]
	fn buf_mut(&mut self) -> &mut ViewBuf<FreelistPage> {
		&mut self.page.buf
	}

	#[inline]
	fn is_full(&self) -> bool {
		(self.buf().length as usize) == self.buf().items.len()
	}

	#[inline]
	fn is_empty(&self) -> bool {
		self.buf().length == 0
	}

	#[inline]
	fn next(&self) -> Option<NonZeroU16> {
		self.buf().next
	}

	#[inline]
	fn page_num(&self) -> NonZeroU16 {
		self.page.page_num
	}

	fn push(&mut self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error> {
		let index: usize = self.buf().length.into();
		self.buf_mut().length += 1;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))?;

		self.buf_mut().items[index] = Some(page_num);
		t.write(self.page_id(), FreelistPage::write_item(self.buf(), index))?;
		Ok(())
	}

	fn pop(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error> {
		if self.buf().length == 0 {
			return Ok(None);
		}

		let index = (self.buf().length - 1) as usize;
		let Some(page_num) = self.buf_mut().items[index].take() else {
			return Err(Error::CorruptedSegment(self.segment_num));
		};
		self.buf_mut().length -= 1;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))?;

		Ok(Some(page_num))
	}

	fn set_next(&mut self, t: &mut Transaction, next: Option<NonZeroU16>) -> Result<(), Error> {
		self.buf_mut().next = next;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))
	}

	fn reset(&mut self, t: &mut Transaction) -> Result<(), Error> {
		self.buf_mut().next = None;
		self.buf_mut().length = 0;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))
	}

	#[inline]
	fn page_id(&self) -> PageId {
		PageId::new(self.segment_num, self.page.page_num.get())
	}
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use crate::{
		cache::PageCache,
		disk::{
			storage::{self, Storage},
			wal::Wal,
		},
		manage::{read::ReadManager, recovery::RecoveryManager, transaction::TransactionManager},
	};

	use super::*;

	#[test]
	fn simple_push() {
		let dir = tempdir().unwrap();
		Storage::init(dir.path(), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));

		let mut freelist_mgr = SegmentManager::new(Arc::clone(&rm), 0).unwrap();

		let mut t = tm.begin();
		freelist_mgr
			.push_freelist(&mut t, NonZeroU16::new(69).unwrap())
			.unwrap();
		freelist_mgr
			.push_freelist(&mut t, NonZeroU16::new(420).unwrap())
			.unwrap();
		t.commit().unwrap();

		let mut header_page: ViewBuf<HeaderPage> = ViewBuf::new();
		rm.read(PageId::new(0, 0), HeaderPage::read(&mut header_page))
			.unwrap();

		assert_eq!(header_page.freelist_trunk, NonZeroU16::new(69));

		let mut freelist_page: ViewBuf<FreelistPage> =
			ViewBuf::new_with_size(rm.page_size().into()).unwrap();
		rm.read(PageId::new(0, 69), FreelistPage::read(&mut freelist_page))
			.unwrap();

		assert_eq!(freelist_page.length, 1);
		assert_eq!(freelist_page.items[0], NonZeroU16::new(420));
	}

	#[test]
	fn simple_push_pop() {
		let dir = tempdir().unwrap();
		Storage::init(dir.path(), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));

		let mut freelist_mgr = SegmentManager::new(Arc::clone(&rm), 0).unwrap();

		let mut t = tm.begin();
		freelist_mgr
			.push_freelist(&mut t, NonZeroU16::new(69).unwrap())
			.unwrap();
		freelist_mgr
			.push_freelist(&mut t, NonZeroU16::new(420).unwrap())
			.unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		assert_eq!(
			freelist_mgr.pop_freelist(&mut t).unwrap(),
			NonZeroU16::new(420)
		);
		assert_eq!(
			freelist_mgr.pop_freelist(&mut t).unwrap(),
			NonZeroU16::new(69)
		);
		assert_eq!(freelist_mgr.pop_freelist(&mut t).unwrap(), None);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_page() {
		let dir = tempdir().unwrap();
		Storage::init(dir.path(), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let mut mgr = SegmentManager::new(Arc::clone(&rm), 0).unwrap();

		let mut t = tm.begin();
		let page = mgr.alloc_page(&mut t).unwrap().unwrap();
		t.commit().unwrap();

		assert_eq!(page, NonZeroU16::new(1).unwrap());
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn alloc_and_free_page() {
		let dir = tempdir().unwrap();
		Storage::init(dir.path(), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);
		let tm = TransactionManager::new(Arc::clone(&cache), recovery);
		let rm = Arc::new(ReadManager::new(Arc::clone(&cache)));
		let mut mgr = SegmentManager::new(Arc::clone(&rm), 0).unwrap();

		let mut t = tm.begin();
		let page = mgr.alloc_page(&mut t).unwrap().unwrap();
		t.commit().unwrap();

		let mut t = tm.begin();
		mgr.free_page(&mut t, page).unwrap();
		t.commit().unwrap();
	}
}
