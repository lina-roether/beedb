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
		!self.freelist_stack.is_empty() || self.header.num_pages != u16::MAX
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
	use std::num::NonZeroU16;

	use byte_view::ViewSlice;
	use mockall::predicate::*;

	use crate::{
		consts::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC},
		manage::{read::MockReadManagerApi, transaction::MockTransactionApi},
		utils::byte_order::ByteOrder,
	};

	use super::*;

	const TEST_PAGE_SIZE: u16 = 16;

	#[test]
	fn factory_build_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.with(eq(PageId::new(69, 0)), always())
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: 1,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		// given
		let factory = SegmentManagerFactory::new(Arc::new(rm));

		// when
		let segment_mgr = factory.build(69).unwrap();

		// then
		assert_eq!(segment_mgr.segment_num(), 69);
	}

	#[test]
	fn factory_build_existing() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: 1,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});
		rm.expect_segment_nums().returning(|| [0, 1, 2].into());

		// given
		let factory = SegmentManagerFactory::new(Arc::new(rm));

		// when
		let segments = factory.build_existing().collect::<Vec<_>>();

		// then
		assert_eq!(segments.len(), 3);
		assert_eq!(segments[0].as_ref().unwrap().segment_num(), 0);
		assert_eq!(segments[1].as_ref().unwrap().segment_num(), 1);
		assert_eq!(segments[2].as_ref().unwrap().segment_num(), 2);
	}

	#[test]
	fn construct_segment_manager_for_new_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: 1,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		// given
		let segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// then
		assert_eq!(segment_mgr.segment_num(), 69);
		assert!(segment_mgr.has_free_pages());
	}

	#[test]
	fn construct_segment_manager_for_existing_full_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: u16::MAX,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		// given
		let segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// then
		assert_eq!(segment_mgr.segment_num(), 69);
		assert!(!segment_mgr.has_free_pages());
	}

	#[test]
	fn construct_segment_manager_for_existing_free_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: NonZeroU16::new(25),
					num_pages: u16::MAX,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});
		rm.expect_read()
			.with(eq(PageId::new(69, 25)), always())
			.returning(|_, op| {
				let freelist_page: &mut ViewSlice<FreelistPage> =
					ViewSlice::new_mut(op.bytes).unwrap();
				freelist_page.next = NonZeroU16::new(420);
				freelist_page.length = freelist_page.items.len() as u16;
				freelist_page.items.fill(NonZeroU16::new(7897));
				Ok(())
			});
		rm.expect_read()
			.with(eq(PageId::new(69, 420)), always())
			.returning(|_, op| {
				let freelist_page: &mut ViewSlice<FreelistPage> =
					ViewSlice::new_mut(op.bytes).unwrap();
				freelist_page.next = None;
				freelist_page.length = 2;
				freelist_page.items[0] = NonZeroU16::new(24);
				freelist_page.items[1] = NonZeroU16::new(25);
				Ok(())
			});

		// given
		let segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// then
		assert_eq!(segment_mgr.segment_num(), 69);
		assert!(segment_mgr.has_free_pages());
	}

	#[test]
	fn alloc_in_new_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: 1,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		let mut t = MockTransactionApi::new();
		t.expect_write()
			.withf(|page_id, op: &WriteOp| {
				*page_id == PageId::new(69, 0)
					&& **ViewSlice::<HeaderPage>::new(op.bytes).unwrap()
						== HeaderPage {
							magic: SEGMENT_MAGIC,
							byte_order: ByteOrder::NATIVE as u8,
							format_version: SEGMENT_FORMAT_VERSION,
							freelist_trunk: None,
							num_pages: 2,
							page_size: TEST_PAGE_SIZE,
						}
			})
			.returning(|_, _| Ok(()));

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		let page_num = segment_mgr.alloc_page(&mut t).unwrap();

		// then
		assert_eq!(page_num, NonZeroU16::new(1));
	}

	#[test]
	fn try_alloc_in_existing_full_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: u16::MAX,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		let page_num = segment_mgr
			.alloc_page(&mut MockTransactionApi::new())
			.unwrap();

		// then
		assert_eq!(page_num, None);
	}

	#[test]
	fn alloc_in_existing_free_segment() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: NonZeroU16::new(420),
					num_pages: u16::MAX,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});
		rm.expect_read()
			.with(eq(PageId::new(69, 420)), always())
			.returning(|_, op| {
				let freelist_page: &mut ViewSlice<FreelistPage> =
					ViewSlice::new_mut(op.bytes).unwrap();
				freelist_page.next = None;
				freelist_page.length = 2;
				freelist_page.items[0] = NonZeroU16::new(24);
				freelist_page.items[1] = NonZeroU16::new(25);
				Ok(())
			});

		let mut t = MockTransactionApi::new();
		t.expect_write()
			.withf(|page_id, op: &WriteOp| {
				*page_id == PageId::new(69, 420)
					&& op.start == 0 && op.bytes == [0_u16.to_ne_bytes(), 1_u16.to_ne_bytes()].concat()
			})
			.returning(|_, _| Ok(()));

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		let page_num = segment_mgr.alloc_page(&mut t).unwrap();

		// then
		assert_eq!(page_num, NonZeroU16::new(25));
	}

	#[test]
	fn alloc_empty_freelist_page() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: NonZeroU16::new(420),
					num_pages: u16::MAX,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});
		rm.expect_read()
			.with(eq(PageId::new(69, 420)), always())
			.returning(|_, op| {
				let freelist_page: &mut ViewSlice<FreelistPage> =
					ViewSlice::new_mut(op.bytes).unwrap();
				freelist_page.next = None;
				freelist_page.length = 0;
				Ok(())
			});

		let mut t = MockTransactionApi::new();
		t.expect_write()
			.withf(|page_id, op: &WriteOp| {
				*page_id == PageId::new(69, 0)
					&& op.start == 0 && **ViewSlice::<HeaderPage>::new(op.bytes).unwrap()
					== HeaderPage {
						magic: SEGMENT_MAGIC,
						byte_order: ByteOrder::NATIVE as u8,
						format_version: SEGMENT_FORMAT_VERSION,
						freelist_trunk: None,
						num_pages: u16::MAX,
						page_size: TEST_PAGE_SIZE,
					}
			})
			.returning(|_, _| Ok(()));

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		let page_num = segment_mgr.alloc_page(&mut t).unwrap();

		// then
		assert_eq!(page_num, NonZeroU16::new(420));
	}

	#[test]
	fn free_page_with_empty_freelist() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: None,
					num_pages: 2,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});

		let mut t = MockTransactionApi::new();
		t.expect_write()
			.withf(|page_id, op| {
				*page_id == PageId::new(69, 1)
					&& op.start == 0 && op.bytes == [0_u16.to_ne_bytes(), 0_u16.to_ne_bytes()].concat()
			})
			.returning(|_, _| Ok(()));
		t.expect_write()
			.withf(|page_id, op| {
				*page_id == PageId::new(69, 0)
					&& op.start == 0 && **ViewSlice::<HeaderPage>::new(op.bytes).unwrap()
					== HeaderPage {
						magic: SEGMENT_MAGIC,
						byte_order: ByteOrder::NATIVE as u8,
						format_version: SEGMENT_FORMAT_VERSION,
						freelist_trunk: NonZeroU16::new(1),
						num_pages: 2,
						page_size: TEST_PAGE_SIZE,
					}
			})
			.returning(|_, _| Ok(()));

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		segment_mgr
			.free_page(&mut t, NonZeroU16::new(1).unwrap())
			.unwrap();
	}

	#[test]
	fn free_page_with_filled_freelist_page() {
		// expect
		let mut rm = MockReadManagerApi::new();
		rm.expect_page_size().returning(|| TEST_PAGE_SIZE);
		rm.expect_read()
			.withf(|page_id, _| page_id.page_num == 0)
			.returning(|_, op| {
				**ViewSlice::new_mut(op.bytes).unwrap() = HeaderPage {
					magic: SEGMENT_MAGIC,
					byte_order: ByteOrder::NATIVE as u8,
					format_version: SEGMENT_FORMAT_VERSION,
					freelist_trunk: NonZeroU16::new(420),
					num_pages: 2,
					page_size: TEST_PAGE_SIZE,
				};
				Ok(())
			});
		rm.expect_read()
			.with(eq(PageId::new(69, 420)), always())
			.returning(|_, op| {
				let freelist_page: &mut ViewSlice<FreelistPage> =
					ViewSlice::new_mut(op.bytes).unwrap();
				freelist_page.next = None;
				freelist_page.length = 2;
				freelist_page.items[0] = NonZeroU16::new(1);
				freelist_page.items[0] = NonZeroU16::new(2);

				Ok(())
			});

		let mut t = MockTransactionApi::new();
		t.expect_write()
			.withf(|page_id, op| {
				*page_id == PageId::new(69, 420)
					&& op.start == 0 && op.bytes == [0_u16.to_ne_bytes(), 3_u16.to_ne_bytes()].concat()
			})
			.returning(|_, _| Ok(()));
		t.expect_write()
			.withf(|page_id, op| {
				*page_id == PageId::new(69, 420) && op.start == 8 && op.bytes == 1_u16.to_ne_bytes()
			})
			.returning(|_, _| Ok(()));

		// given
		let mut segment_mgr = SegmentManager::new(Arc::new(rm), 69).unwrap();

		// when
		segment_mgr
			.free_page(&mut t, NonZeroU16::new(1).unwrap())
			.unwrap();
	}
}
