use std::sync::Arc;

use crate::{cache::PageCache, disk::storage::StorageApi, id::PageId, pages::ReadOp};

use super::err::Error;

pub(super) struct ReadManager<Storage>
where
	Storage: StorageApi,
{
	cache: Arc<PageCache<Storage>>,
}

impl<Storage> ReadManager<Storage>
where
	Storage: StorageApi,
{
	pub fn new(cache: Arc<PageCache<Storage>>) -> Self {
		Self { cache }
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.cache.page_size()
	}

	#[inline]
	pub fn segment_nums(&self) -> Box<[u32]> {
		self.cache.segment_nums()
	}

	pub fn read(&self, page_id: PageId, op: ReadOp) -> Result<(), Error> {
		let page = self.cache.read_page(page_id)?;
		debug_assert!(op.range().end <= page.len());

		op.bytes.copy_from_slice(&page[op.range()]);
		Ok(())
	}
}
