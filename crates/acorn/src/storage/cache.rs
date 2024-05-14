use std::{
	alloc::{alloc_zeroed, dealloc, Layout},
	collections::HashMap,
	mem,
	ops::{Deref, DerefMut},
	ptr,
	sync::atomic::{AtomicUsize, Ordering},
};

use parking_lot::{lock_api::RawRwLock as _, RawRwLock, RwLock};
use static_assertions::assert_impl_all;

#[cfg(test)]
use mockall::automock;

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

#[derive(Clone)]
pub(crate) struct PageReadGuard<'a> {
	page: &'a [u8],
	lock: &'a RawRwLock,
}

impl<'a> Deref for PageReadGuard<'a> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a> Drop for PageReadGuard<'a> {
	fn drop(&mut self) {
		// Safety: the existence of this object guarantees the lock is owned by the
		// current context
		unsafe { self.lock.unlock_shared() };
	}
}

pub(crate) struct PageWriteGuard<'a> {
	page: &'a mut [u8],
	lock: &'a RawRwLock,
}

impl<'a> Deref for PageWriteGuard<'a> {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a> DerefMut for PageWriteGuard<'a> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.page
	}
}

impl<'a> Drop for PageWriteGuard<'a> {
	fn drop(&mut self) {
		// Safety: the existence of this object guarantees the lock is owned by the
		// current context
		unsafe { self.lock.unlock_exclusive() };
	}
}

pub(crate) struct PageCache {
	buf: PageBuffer,
	indices: RwLock<HashMap<PageId, usize>>,
	replacer: RwLock<CacheReplacer<PageId>>,
	locks: Box<[RawRwLock]>,
}
assert_impl_all!(PageCache: Send, Sync);

// Safety: `buf`'s internal pointer is never leaked in any form.
unsafe impl Send for PageCache {}

// Safety: `buf` is only accessed through the `load` and `store` methods,
// which guarantee the safety of the references by acquiring the corresponding
// locks.
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
			locks: std::iter::repeat_with(|| RawRwLock::INIT)
				.take(num_pages)
				.collect(),
		}
	}
}

#[cfg_attr(test, automock)]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait PageCacheApi {
	fn load<'a>(&'a self, page_id: PageId) -> Option<PageReadGuard<'a>>;
	fn store<'a>(&'a self, page_id: PageId) -> PageWriteGuard<'a>;
}

impl PageCacheApi for PageCache {
	fn load(&self, page_id: PageId) -> Option<PageReadGuard<'_>> {
		let indices = self.indices.read();
		let index = indices.get(&page_id).copied()?;
		mem::drop(indices);

		let replacer = self.replacer.read();
		let access_successful = replacer.access(&page_id);
		debug_assert!(access_successful);
		mem::drop(replacer);

		let lock = &self.locks[index];
		lock.lock_shared();
		// Safety: The safety of the reference is guaranteed by acquiring the shared
		// lock.
		let page =
			unsafe { self.buf.get_page(index) }.expect("Tried to index page buffer out of bounds!");

		Some(PageReadGuard { lock, page })
	}

	fn store(&self, page_id: PageId) -> PageWriteGuard<'_> {
		let mut indices = self.indices.write();
		let index = indices.get(&page_id).copied().unwrap_or_else(|| {
			let mut replacer = self.replacer.write();
			let maybe_evicted = replacer.evict_replace(page_id);
			mem::drop(replacer);

			if let Some(evicted) = maybe_evicted {
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
			}
		});

		let lock = &self.locks[index];
		lock.lock_exclusive();
		// Safety: The safety of the reference is guaranteed by acquiring the exclusive
		// lock.
		let page = unsafe { self.buf.get_page_mut(index) }
			.expect("Triet to index page buffer out of bounds!");

		PageWriteGuard { lock, page }
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
		cache
			.store(page_id!(69, 420))
			.copy_from_slice(&expected_page);

		let mut received_page = [0; PAGE_BODY_SIZE];
		received_page.copy_from_slice(&cache.load(page_id!(69, 420)).unwrap());

		// then
		assert_buf_eq!(expected_page, received_page);
	}

	#[test]
	fn load_cache_miss() {
		// given
		let cache = PageCache::new(&PageCacheConfig {
			page_cache_size: 2 * MIB,
		});

		// when
		let guard = cache.load(page_id!(69, 420));

		// then
		assert!(guard.is_none())
	}
}
