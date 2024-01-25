use std::sync::Arc;

use crate::{cache::PageCache, id::PageId};

use super::err::Error;

pub(super) struct ReadManager {
	cache: Arc<PageCache>,
}

impl ReadManager {
	pub fn new(cache: Arc<PageCache>) -> Self {
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

	pub fn read(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), Error> {
		let page = self.cache.read_page(page_id)?;
		debug_assert!(buf.len() <= page.len());

		buf.copy_from_slice(&page[0..buf.len()]);
		Ok(())
	}
}
