use std::ops::{Index, IndexMut};

use crate::{consts::DEFAULT_PAGE_CACHE_SIZE, files::segment::PAGE_BODY_SIZE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageCacheConfig {
	pub page_cache_size: usize,
}

impl Default for PageCacheConfig {
	fn default() -> Self {
		Self {
			page_cache_size: DEFAULT_PAGE_CACHE_SIZE,
		}
	}
}

struct PageBuffer {
	buf: Box<[u8]>,
	num_pages: usize,
}

impl PageBuffer {
	fn new(config: &PageCacheConfig) -> Self {
		let num_pages = config.page_cache_size / PAGE_BODY_SIZE;
		let buf_size = num_pages * PAGE_BODY_SIZE;

		Self {
			buf: vec![0; buf_size].into_boxed_slice(),
			num_pages,
		}
	}
}

impl Index<usize> for PageBuffer {
	type Output = [u8];

	fn index(&self, index: usize) -> &Self::Output {
		&self.buf[index * PAGE_BODY_SIZE..(index + 1) * PAGE_BODY_SIZE]
	}
}

impl IndexMut<usize> for PageBuffer {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.buf[index * PAGE_BODY_SIZE..(index + 1) * PAGE_BODY_SIZE]
	}
}

