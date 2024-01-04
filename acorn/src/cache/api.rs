use crate::{disk, index::PageId};

use super::{PageReadGuard, PageWriteGuard};

#[cfg(test)]
use mockall::automock;

#[allow(clippy::needless_lifetimes)]
#[cfg_attr(test, automock)]
pub trait PageCache {
	fn read_page<'a>(&'a self, page_id: PageId) -> Result<PageReadGuard<'a>, disk::Error>;

	fn write_page<'a>(&'a self, page_id: PageId) -> Result<PageWriteGuard<'a>, disk::Error>;

	fn num_dirty(&self) -> usize;

	fn flush(&self) -> Result<(), disk::Error>;
}
