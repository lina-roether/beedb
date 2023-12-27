use core::slice;
use std::{
	alloc::{alloc, dealloc, handle_alloc_error, Layout},
	cell::UnsafeCell,
	iter,
	ops::{Deref, DerefMut},
	ptr::NonNull,
	sync::atomic::{AtomicBool, AtomicUsize, Ordering},
	usize,
};

use parking_lot::{lock_api::RawRwLock as _, Mutex, RawRwLock};

pub struct PageReadGuard<'a> {
	lock: &'a RawRwLock,
	page: &'a [u8],
}

impl<'a> Drop for PageReadGuard<'a> {
	fn drop(&mut self) {
		unsafe {
			self.lock.unlock_shared();
		}
	}
}

impl<'a> Deref for PageReadGuard<'a> {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a> AsRef<[u8]> for PageReadGuard<'a> {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self
	}
}

pub struct PageWriteGuard<'a> {
	lock: &'a RawRwLock,
	page: &'a mut [u8],
}

impl<'a> Drop for PageWriteGuard<'a> {
	fn drop(&mut self) {
		unsafe {
			self.lock.unlock_exclusive();
		}
	}
}

impl<'a> Deref for PageWriteGuard<'a> {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a> DerefMut for PageWriteGuard<'a> {
	#[inline]
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.page
	}
}

impl<'a> AsRef<[u8]> for PageWriteGuard<'a> {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self
	}
}

impl<'a> AsMut<[u8]> for PageWriteGuard<'a> {
	#[inline]
	fn as_mut(&mut self) -> &mut [u8] {
		self
	}
}

pub struct PageBuffer {
	length: usize,
	page_size: usize,
	page_size_padded: usize,
	meta: Box<[PageMeta]>,
	freelist: Mutex<Vec<usize>>,
	last_filled: AtomicUsize,
	pages: UnsafeCell<NonNull<u8>>,
}

impl PageBuffer {
	const PAGE_ALIGNMENT: usize = 8;

	pub fn new(page_size: usize, length: usize) -> Self {
		let (buf_layout, page_size_padded) = Self::page_buffer_layout(page_size, length);
		let Some(pages) = (unsafe { NonNull::new(alloc(buf_layout)) }) else {
			handle_alloc_error(buf_layout);
		};

		Self {
			length,
			page_size,
			page_size_padded,
			meta: iter::repeat_with(PageMeta::default).take(length).collect(),
			freelist: Mutex::new(Vec::new()),
			last_filled: AtomicUsize::new(0),
			pages: UnsafeCell::new(pages),
		}
	}

	pub fn free_page(&self, index: usize) {
		let meta = &self.meta[index];
		meta.lock.lock_exclusive();
		if !meta.occupied.load(Ordering::Acquire) {
			return;
		}
		meta.occupied.store(false, Ordering::Release);
		self.freelist.lock().push(index);
		unsafe { meta.lock.unlock_exclusive() }
	}

	pub fn has_space(&self) -> bool {
		self.last_filled.load(Ordering::Relaxed) < self.length || self.freelist.lock().len() != 0
	}

	pub fn allocate_page(&self) -> Option<usize> {
		let last_filled = self.last_filled.load(Ordering::Acquire);
		let allocated_idx = if last_filled < self.length {
			self.last_filled.store(last_filled + 1, Ordering::Release);
			last_filled
		} else {
			self.freelist.lock().pop()?
		};
		self.meta[allocated_idx]
			.occupied
			.store(true, Ordering::Relaxed);
		Some(allocated_idx)
	}

	pub fn read_page(&self, index: usize) -> Option<PageReadGuard> {
		let meta = &self.meta[index];
		if !meta.occupied.load(Ordering::Relaxed) {
			return None;
		};
		meta.lock.lock_shared();
		Some(PageReadGuard {
			lock: &meta.lock,
			page: unsafe { slice::from_raw_parts(self.get_page_ptr(index), self.page_size) },
		})
	}

	pub fn write_page(&self, index: usize) -> Option<PageWriteGuard> {
		let meta = &self.meta[index];
		if !meta.occupied.load(Ordering::Relaxed) {
			return None;
		};
		meta.lock.lock_exclusive();
		Some(PageWriteGuard {
			lock: &meta.lock,
			page: unsafe { slice::from_raw_parts_mut(self.get_page_ptr(index), self.page_size) },
		})
	}

	unsafe fn get_page_ptr(&self, index: usize) -> *mut u8 {
		if index >= self.length {
			panic!(
				"Page buffer index {index} out of bounds for length {}",
				self.length
			);
		}
		(*self.pages.get())
			.as_ptr()
			.add(index * self.page_size_padded)
	}

	fn page_buffer_layout(page_size: usize, length: usize) -> (Layout, usize) {
		let page_layout = Layout::from_size_align(page_size, Self::PAGE_ALIGNMENT).unwrap();
		page_layout.repeat(length).unwrap()
	}
}

impl Drop for PageBuffer {
	fn drop(&mut self) {
		unsafe {
			dealloc(
				(*self.pages.get()).as_ptr(),
				Self::page_buffer_layout(self.page_size, self.length).0,
			)
		}
	}
}

struct PageMeta {
	occupied: AtomicBool,
	lock: RawRwLock,
}

impl Default for PageMeta {
	fn default() -> Self {
		Self {
			occupied: AtomicBool::new(false),
			lock: RawRwLock::INIT,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn allocate_read_and_write_pages() {
		let buffer = PageBuffer::new(4, 10);

		let idx_1 = buffer.allocate_page().unwrap();
		let idx_2 = buffer.allocate_page().unwrap();

		{
			let mut page_1 = buffer.write_page(idx_1).unwrap();
			page_1.copy_from_slice(b"moin");
		}

		{
			let mut page_2 = buffer.write_page(idx_2).unwrap();
			page_2.copy_from_slice(b"tree");
		}

		let page_1 = buffer.read_page(idx_1).unwrap();
		let page_2 = buffer.read_page(idx_2).unwrap();

		assert_eq!(*page_1, *b"moin");
		assert_eq!(*page_2, *b"tree");
	}

	#[test]
	fn try_access_freed_index() {
		let buffer = PageBuffer::new(4, 10);

		let idx = buffer.allocate_page().unwrap();
		buffer.free_page(idx);

		assert!(buffer.read_page(idx).is_none());
	}

	#[test]
	fn fills_all_slots() {
		let buffer = PageBuffer::new(4, 10);

		let mut allocated: Vec<usize> = Vec::new();
		while let Some(idx) = buffer.allocate_page() {
			allocated.push(idx);
		}

		for idx in allocated.iter().copied() {
			buffer.free_page(idx);
		}

		let mut num_reallocated = 0;
		while buffer.allocate_page().is_some() {
			num_reallocated += 1;
		}

		assert_eq!(allocated.len(), 10);
		assert_eq!(num_reallocated, 10);
	}
}
