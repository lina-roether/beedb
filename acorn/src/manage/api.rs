use std::num::NonZeroU16;

use crate::{
	cache::{PageReadGuard, PageWriteGuard},
	disk,
	index::PageId,
};

use super::{
	rw::WriteError,
	segment_alloc,
	transaction::{self, Operation},
};

pub trait TransactionManager {
	fn begin(&self) -> Result<u64, transaction::Error>;

	fn operation(
		&self,
		tid: u64,
		operation: Operation,
		before: &[u8],
		after: &[u8],
	) -> Result<(), transaction::Error>;

	fn commit(&self, tid: u64) -> Result<(), transaction::Error>;
}

pub trait PageRwManager {
	fn read_page(&self, page_id: PageId) -> Result<PageReadGuard, disk::Error>;

	fn write_page(
		&self,
		tid: u64,
		page_id: PageId,
		write_fn: impl FnOnce(&mut PageWriteGuard),
	) -> Result<(), WriteError>;
}

pub trait SegmentAllocManager {
	fn segment_num(&self) -> u32;

	fn alloc_page(&self, tid: u64) -> Result<Option<NonZeroU16>, segment_alloc::Error>;

	fn free_page(&self, tid: u64, page_num: NonZeroU16) -> Result<(), segment_alloc::Error>;
}
