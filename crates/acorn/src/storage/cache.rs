use std::{
	collections::HashMap,
	ops::{Index, IndexMut},
};

use crate::{
	consts::DEFAULT_PAGE_CACHE_SIZE, files::segment::PAGE_BODY_SIZE, utils::cache::CacheReplacer,
};

use super::PageId;

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
	num_filled: usize,
}

impl PageBuffer {
	fn new(num_pages: usize) -> Self {
		let buf_size = num_pages * PAGE_BODY_SIZE;
		Self {
			buf: vec![0; buf_size].into_boxed_slice(),
			num_pages,
			num_filled: 0,
		}
	}

	#[inline]
	fn as_slice(&self) -> &[u8] {
		&self.buf[0..self.num_filled * PAGE_BODY_SIZE]
	}

	#[inline]
	fn as_mut_slice(&mut self) -> &mut [u8] {
		&mut self.buf[0..self.num_filled * PAGE_BODY_SIZE]
	}

	fn push_page(&mut self) -> Option<&mut [u8]> {
		if self.num_filled == self.num_pages {
			return None;
		}
		let index = self.num_filled;
		self.num_filled += 1;
		Some(&mut self[index])
	}
}

impl Index<usize> for PageBuffer {
	type Output = [u8];

	fn index(&self, index: usize) -> &Self::Output {
		&self.as_slice()[index * PAGE_BODY_SIZE..(index + 1) * PAGE_BODY_SIZE]
	}
}

impl IndexMut<usize> for PageBuffer {
	fn index_mut(&mut self, index: usize) -> &mut Self::Output {
		&mut self.as_mut_slice()[index * PAGE_BODY_SIZE..(index + 1) * PAGE_BODY_SIZE]
	}
}

pub(super) struct PageCache {
	buf: PageBuffer,
	indices: HashMap<PageId, usize>,
	replacer: CacheReplacer<PageId>,
}

impl PageCache {
	pub fn new(config: &PageCacheConfig) -> Self {
		let num_pages = config.page_cache_size / PAGE_BODY_SIZE;
		let buf = PageBuffer::new(num_pages);
		let replacer = CacheReplacer::new(num_pages);
		Self {
			buf,
			replacer,
			indices: HashMap::new(),
		}
	}

	pub fn load(&self, page_id: PageId, buf: &mut [u8]) -> bool {
		let Some(index) = self.indices.get(&page_id) else {
			return false;
		};
		let access_successful = self.replacer.access(&page_id);
		debug_assert!(access_successful);

		buf.copy_from_slice(&self.buf[*index]);

		true
	}

	pub fn store(&mut self, page_id: PageId, buf: &[u8]) {
		if self.indices.contains_key(&page_id) {
			return;
		}
		if let Some(evicted) = self.replacer.evict_replace(page_id) {
			let index = self
				.indices
				.remove(&evicted)
				.expect("Tried to evict a page that is not in the cache!");
			self.indices.insert(page_id, index);
			self.buf[index].copy_from_slice(buf);
		} else {
			self.indices.insert(page_id, self.buf.num_filled);
			self.buf
				.push_page()
				.expect("Failed to evict a page when the buffer was full!")
				.copy_from_slice(buf);
		}
	}
}

#[cfg(test)]
mod tests {
	use pretty_assertions::assert_buf_eq;

	use crate::{storage::test_helpers::page_id, utils::units::MIB};

	use super::*;

	#[test]
	fn load_and_store() {
		// given
		let mut cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let expected_page = [69; PAGE_BODY_SIZE];
		cache.store(page_id!(69, 420), &expected_page);
		let mut received_page = [0; PAGE_BODY_SIZE];
		let success = cache.load(page_id!(69, 420), &mut received_page);

		// then
		assert!(success);
		assert_buf_eq!(expected_page, received_page);
	}

	#[test]
	fn load_cache_miss() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let mut received_page = [0; PAGE_BODY_SIZE];
		let success = cache.load(page_id!(69, 420), &mut received_page);

		// then
		assert!(!success);
		assert_buf_eq!(received_page, [0; PAGE_BODY_SIZE]);
	}
}
