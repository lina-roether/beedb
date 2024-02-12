use std::sync::Arc;

#[cfg(test)]
use mockall::automock;

use crate::{
	cache::{PageCache, PageCacheApi},
	id::PageId,
	pages::ReadOp,
};

use super::err::Error;

#[allow(clippy::needless_lifetimes)]
#[cfg_attr(test, automock)]
pub(super) trait ReadManagerApi {
	fn page_size(&self) -> u16;

	fn segment_nums(&self) -> Box<[u32]>;

	fn read<'a>(&self, page_id: PageId, op: ReadOp<'a>) -> Result<(), Error>;
}

pub(super) struct ReadManager<PageCache = self::PageCache>
where
	PageCache: PageCacheApi,
{
	cache: Arc<PageCache>,
}

impl<PageCache> ReadManager<PageCache>
where
	PageCache: PageCacheApi,
{
	pub fn new(cache: Arc<PageCache>) -> Self {
		Self { cache }
	}
}

impl<PageCache> ReadManagerApi for ReadManager<PageCache>
where
	PageCache: PageCacheApi,
{
	#[inline]
	fn page_size(&self) -> u16 {
		self.cache.page_size()
	}

	#[inline]
	fn segment_nums(&self) -> Box<[u32]> {
		self.cache.segment_nums()
	}

	fn read(&self, page_id: PageId, op: ReadOp) -> Result<(), Error> {
		let page = self.cache.read_page(page_id)?;
		debug_assert!(op.range().end <= page.len());

		op.bytes.copy_from_slice(&page[op.range()]);
		Ok(())
	}
}
