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

#[cfg(test)]
mod tests {
	use crate::cache::MockPageCacheApi;

	use mockall::predicate::*;

	use super::*;

	#[test]
	fn get_page_size() {
		// expect
		let mut page_cache = MockPageCacheApi::new();
		page_cache.expect_page_size().returning(|| 420);

		// given
		let rm = ReadManager::new(Arc::new(page_cache));

		// when
		let page_size = rm.page_size();

		// then
		assert_eq!(page_size, 420);
	}

	#[test]
	fn get_segment_nums() {
		// expect
		let mut page_cache = MockPageCacheApi::new();
		page_cache
			.expect_segment_nums()
			.returning(|| vec![25, 69, 420].into());

		// given
		let rm = ReadManager::new(Arc::new(page_cache));

		// when
		let segment_nums = rm.segment_nums();

		// then
		assert_eq!(segment_nums, vec![25, 69, 420].into());
	}

	#[test]
	fn read() {
		// expect
		let mut page_cache = MockPageCacheApi::new();
		page_cache.expect_page_size().returning(|| 16);
		page_cache
			.expect_read_page()
			.with(eq(PageId::new(69, 420)))
			.returning(|_| Ok((0..16).collect()));

		// given
		let rm = ReadManager::new(Arc::new(page_cache));
		let mut buf = vec![0; 4];

		// when
		rm.read(PageId::new(69, 420), ReadOp::new(3, &mut buf))
			.unwrap();

		// then
		assert_eq!(buf, vec![3, 4, 5, 6]);
	}
}
