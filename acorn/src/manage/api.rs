#[cfg(test)]
use mockall::automock;

use std::num::NonZeroU16;

use crate::{cache::PageReadGuard, index::PageId};

use super::{err::Error, rw::PageWriteHandle, transaction::Operation};

#[cfg_attr(test, automock)]
pub trait TransactionManager {
	fn begin(&self) -> Result<u64, Error>;

	fn operation(
		&self,
		tid: u64,
		operation: Operation,
		before: &[u8],
		after: &[u8],
	) -> Result<(), Error>;

	fn commit(&self, tid: u64) -> Result<(), Error>;

	fn assert_valid_tid(&self, tid: u64) -> Result<(), Error>;
}

#[allow(clippy::needless_lifetimes)]
// #[cfg_attr(test, automock)]
pub trait PageRwManager<TMgr>
where
	TMgr: TransactionManager,
{
	fn read_page<'a>(&'a self, page_id: PageId) -> Result<PageReadGuard<'a>, Error>;

	fn write_page(&self, tid: u64, page_id: PageId) -> Result<PageWriteHandle<TMgr>, Error>;
}

pub trait SegmentAllocManager {
	fn segment_num(&self) -> u32;

	fn alloc_page(&self, tid: u64) -> Result<Option<NonZeroU16>, Error>;

	fn free_page(&self, tid: u64, page_num: NonZeroU16) -> Result<(), Error>;
}
