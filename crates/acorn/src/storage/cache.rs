use std::{
	alloc::{alloc_zeroed, dealloc, Layout},
	collections::HashMap,
	mem, ptr,
	sync::atomic::{AtomicUsize, Ordering},
};

use parking_lot::RwLock;
use static_assertions::assert_impl_all;

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
	buf: *mut u8,
	num_pages: usize,
	num_filled: AtomicUsize,
}

impl PageBuffer {
	fn new(num_pages: usize) -> Self {
		let buf_size = num_pages * PAGE_BODY_SIZE;
		let buf = if buf_size != 0 {
			// Safety: buf_size is guaranteed not to be zero, so the layout is not
			// zero-sized.
			unsafe { alloc_zeroed(Layout::from_size_align(buf_size, 1).unwrap()) }
		} else {
			ptr::null_mut()
		};
		Self {
			buf,
			num_pages,
			num_filled: AtomicUsize::new(0),
		}
	}

	fn push_page(&self) -> Option<usize> {
		let num_filled = self.num_filled.load(Ordering::Acquire);
		if num_filled == self.num_pages {
			return None;
		}
		self.num_filled.store(num_filled + 1, Ordering::Release);
		Some(num_filled)
	}

	fn page_ptr(&self, index: usize) -> Option<*mut u8> {
		if index >= self.num_pages {
			return None;
		}
		// Safety: the resulting pointer is guaranteed to be in the allocated buffer.
		Some(unsafe { self.buf.add(index * PAGE_BODY_SIZE) })
	}

	/// # Safety:
	/// The caller must ensure that no mutable reference to the same page
	/// exists.
	unsafe fn get_page(&self, index: usize) -> Option<&[u8]> {
		Some(std::slice::from_raw_parts(
			self.page_ptr(index)?,
			PAGE_BODY_SIZE,
		))
	}

	/// # Safety:
	/// The caller must ensure that no shared reference, and no other mutable
	/// references to the same page exist.
	unsafe fn get_page_mut(&self, index: usize) -> Option<&mut [u8]> {
		Some(std::slice::from_raw_parts_mut(
			self.page_ptr(index)?,
			PAGE_BODY_SIZE,
		))
	}
}

impl Drop for PageBuffer {
	fn drop(&mut self) {
		if !self.buf.is_null() {
			let buf_size = self.num_pages * PAGE_BODY_SIZE;
			// Safety:
			// - `self.buf` is guaranteed not to be null
			// - The buffer is never reallocated, so the layout stays the same
			unsafe { dealloc(self.buf, Layout::from_size_align(buf_size, 1).unwrap()) }
		}
	}
}

pub(super) struct PageCache {
	buf: PageBuffer,
	indices: RwLock<HashMap<PageId, usize>>,
	replacer: RwLock<CacheReplacer<PageId>>,
}
assert_impl_all!(PageCache: Send, Sync);

// Safety: `buf`'s internal pointer is never leaked in any form.
unsafe impl Send for PageCache {}

// Safety: `buf` is only accessed through the `load` and `store` methods,
// which are unsafe, and explicitly require that there be no references
// on other threads that would make this unsound.
unsafe impl Sync for PageCache {}

impl PageCache {
	pub fn new(config: &PageCacheConfig) -> Self {
		let num_pages = config.page_cache_size / PAGE_BODY_SIZE;
		let buf = PageBuffer::new(num_pages);
		let replacer = CacheReplacer::new(num_pages);
		Self {
			buf,
			replacer: RwLock::new(replacer),
			indices: RwLock::new(HashMap::new()),
		}
	}

	/// # Safety:
	/// The caller must ensure that no thread is currently mutating the page in
	/// the cache.
	pub unsafe fn load(&self, page_id: PageId, buf: &mut [u8]) -> bool {
		let indices = self.indices.read();
		let Some(index) = indices.get(&page_id).copied() else {
			return false;
		};
		mem::drop(indices);

		let replacer = self.replacer.read();
		let access_successful = replacer.access(&page_id);
		debug_assert!(access_successful);
		mem::drop(replacer);

		buf.copy_from_slice(
			// Safety: As long as no other tread holds one, no mutable reference to this page can
			// exist, because references to pages never outlive the `store` function.
			self.buf
				.get_page(index)
				.expect("Tried to index page buffer out of bounds!"),
		);

		true
	}

	/// # Safety:
	/// The caller must ensure that no other thread is currently accessing this
	/// page in the cache.
	pub unsafe fn store(&self, page_id: PageId, buf: &[u8]) {
		let mut indices = self.indices.write();
		if indices.contains_key(&page_id) {
			return;
		}

		let mut replacer = self.replacer.write();
		let maybe_evicted = replacer.evict_replace(page_id);
		mem::drop(replacer);

		let index = if let Some(evicted) = maybe_evicted {
			let index = indices
				.remove(&evicted)
				.expect("Tried to evict a page that is not in the cache!");
			indices.insert(page_id, index);
			index
		} else {
			let index = self
				.buf
				.push_page()
				.expect("Failed to evict a page when the buffer was full!");
			indices.insert(page_id, index);
			index
		};

		// Safety: As long as no other thread holds one, no other reference to this page
		// can exist, because references to pages never outlive the `load` and `store`
		// functions.
		self.buf
			.get_page_mut(index)
			.expect("Triet to index page buffer out of bounds!")
			.copy_from_slice(buf);
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
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let expected_page = [69; PAGE_BODY_SIZE];
		unsafe { cache.store(page_id!(69, 420), &expected_page) };
		let mut received_page = [0; PAGE_BODY_SIZE];
		let success = unsafe { cache.load(page_id!(69, 420), &mut received_page) };

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
		let success = unsafe { cache.load(page_id!(69, 420), &mut received_page) };

		// then
		assert!(!success);
		assert_buf_eq!(received_page, [0; PAGE_BODY_SIZE]);
	}
}
