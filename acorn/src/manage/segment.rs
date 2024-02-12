use std::{num::NonZeroU16, sync::Arc};

use byte_view::{BufError, ViewBuf};

#[cfg(test)]
use mockall::{automock, concretize};

use crate::{
	id::PageId,
	pages::{FreelistPage, HeaderPage, WriteOp},
};

use super::{
	err::Error,
	read::{ReadManager, ReadManagerApi},
	transaction::TransactionApi,
};

#[cfg_attr(test, automock(
    type SegmentManager = MockSegmentManagerApi;
    type BuildIter = std::vec::IntoIter<Result<MockSegmentManagerApi, Error>>;
))]
pub(super) trait SegmentManagerFactoryApi {
	type SegmentManager: SegmentManagerApi;
	type BuildIter: Iterator<Item = Result<Self::SegmentManager, Error>>;

	fn build(&self, segment_num: u32) -> Result<Self::SegmentManager, Error>;

	fn build_existing(&self) -> Self::BuildIter;
}

#[cfg_attr(test, automock)]
pub(super) trait SegmentManagerApi {
	fn segment_num(&self) -> u32;

	#[cfg_attr(test, concretize)]
	fn alloc_page<Transaction>(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error>
	where
		Transaction: TransactionApi;

	#[cfg_attr(test, concretize)]
	fn free_page<Transaction>(
		&mut self,
		t: &mut Transaction,
		page_num: NonZeroU16,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi;

	fn has_free_pages(&self) -> bool;
}

pub(super) struct SegmentManagerFactory<ReadManager = self::ReadManager>
where
	ReadManager: ReadManagerApi,
{
	rm: Arc<ReadManager>,
}

impl<ReadManager> SegmentManagerFactory<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	pub fn new(rm: Arc<ReadManager>) -> Self {
		Self { rm }
	}
}

impl<ReadManager> SegmentManagerFactoryApi for SegmentManagerFactory<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	type SegmentManager = SegmentManager<ReadManager>;
	type BuildIter = BuildIter<ReadManager>;

	fn build(&self, segment_num: u32) -> Result<Self::SegmentManager, Error> {
		SegmentManager::new(Arc::clone(&self.rm), segment_num)
	}

	fn build_existing(&self) -> BuildIter<ReadManager> {
		BuildIter {
			rm: Arc::clone(&self.rm),
			segment_nums: self.rm.segment_nums().into_vec().into_iter(),
		}
	}
}

pub(super) struct BuildIter<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	rm: Arc<ReadManager>,
	segment_nums: std::vec::IntoIter<u32>,
}

impl<ReadManager> Iterator for BuildIter<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	type Item = Result<SegmentManager<ReadManager>, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		let segment_num = self.segment_nums.next()?;
		Some(SegmentManager::new(Arc::clone(&self.rm), segment_num))
	}
}

pub(super) struct SegmentManager<ReadManager = self::ReadManager>
where
	ReadManager: ReadManagerApi,
{
	segment_num: u32,
	rm: Arc<ReadManager>,
	header: ViewBuf<HeaderPage>,
	freelist_stack: Vec<FreelistStackEntry>,
}

impl<ReadManager> SegmentManager<ReadManager>
where
	ReadManager: ReadManagerApi,
{
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
}

impl<ReadManager> SegmentManagerApi for SegmentManager<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	fn segment_num(&self) -> u32 {
		self.segment_num
	}

	fn alloc_page<Transaction>(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error>
	where
		Transaction: TransactionApi,
	{
		if let Some(free_page) = self.pop_freelist(t)? {
			return Ok(Some(free_page));
		}
		if let Some(new_page) = self.create_new_page(t)? {
			return Ok(Some(new_page));
		}
		Ok(None)
	}

	fn free_page<Transaction>(
		&mut self,
		t: &mut Transaction,
		page_num: NonZeroU16,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		self.push_freelist(t, page_num)
	}

	fn has_free_pages(&self) -> bool {
		!self.freelist_stack.is_empty()
	}
}

impl<ReadManager> SegmentManager<ReadManager>
where
	ReadManager: ReadManagerApi,
{
	fn create_new_page<Transaction>(
		&mut self,
		t: &mut Transaction,
	) -> Result<Option<NonZeroU16>, Error>
	where
		Transaction: TransactionApi,
	{
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

	fn push_freelist<Transaction>(
		&mut self,
		t: &mut Transaction,
		page_num: NonZeroU16,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
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

	fn pop_freelist<Transaction>(
		&mut self,
		t: &mut Transaction,
	) -> Result<Option<NonZeroU16>, Error>
	where
		Transaction: TransactionApi,
	{
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

	fn push_trunk<Transaction>(
		&mut self,
		t: &mut Transaction,
		page_num: NonZeroU16,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		self.set_trunk(t, Some(page_num))?;
		self.freelist_stack
			.push(FreelistStackEntry::new(page_num, self.rm.page_size()).unwrap());
		let mut new_trunk = self.get_trunk().unwrap();
		new_trunk.reset(t)?;
		Ok(())
	}

	fn set_trunk<Transaction>(
		&mut self,
		t: &mut Transaction,
		trunk_num: Option<NonZeroU16>,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		self.header.freelist_trunk = trunk_num;
		self.write_header(t)?;
		Ok(())
	}

	fn write_header<Transaction>(&self, t: &mut Transaction) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		self.write(t, 0, HeaderPage::write(&self.header))
	}

	fn write<Transaction>(
		&self,
		t: &mut Transaction,
		page_num: u16,
		op: WriteOp,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
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

	fn push<Transaction>(&mut self, t: &mut Transaction, page_num: NonZeroU16) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		let index: usize = self.buf().length.into();
		self.buf_mut().length += 1;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))?;

		self.buf_mut().items[index] = Some(page_num);
		t.write(self.page_id(), FreelistPage::write_item(self.buf(), index))?;
		Ok(())
	}

	fn pop<Transaction>(&mut self, t: &mut Transaction) -> Result<Option<NonZeroU16>, Error>
	where
		Transaction: TransactionApi,
	{
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

	fn set_next<Transaction>(
		&mut self,
		t: &mut Transaction,
		next: Option<NonZeroU16>,
	) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
		self.buf_mut().next = next;
		t.write(self.page_id(), FreelistPage::write_header(self.buf()))
	}

	fn reset<Transaction>(&mut self, t: &mut Transaction) -> Result<(), Error>
	where
		Transaction: TransactionApi,
	{
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
		manage::{
			read::ReadManager,
			recovery::RecoveryManager,
			transaction::{TransactionManager, TransactionManagerApi as _},
		},
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
