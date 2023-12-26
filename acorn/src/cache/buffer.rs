use core::slice;
use std::{
	alloc::{alloc, dealloc, handle_alloc_error, Layout},
	cell::UnsafeCell,
	iter,
	ops::{Deref, DerefMut},
	ptr::NonNull,
	usize,
};

use parking_lot::{lock_api::RawRwLock as _, RawRwLock};

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
	locks: Box<[RawRwLock]>,
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
			locks: iter::repeat_with(|| RawRwLock::INIT).take(length).collect(),
			pages: UnsafeCell::new(pages),
		}
	}

	pub fn read_page(&self, index: usize) -> PageReadGuard {
		self.locks[index].lock_shared();
		PageReadGuard {
			lock: &self.locks[index],
			page: unsafe { slice::from_raw_parts(self.get_page_ptr(index), self.page_size) },
		}
	}

	pub fn write_page(&self, index: usize) -> PageWriteGuard {
		self.locks[index].lock_exclusive();
		PageWriteGuard {
			lock: &self.locks[index],
			page: unsafe { slice::from_raw_parts_mut(self.get_page_ptr(index), self.page_size) },
		}
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn page_buffer_construction() {
		let _buffer = PageBuffer::new(69, 420);
	}

	#[test]
	fn read_and_write_pages() {
		let buffer = PageBuffer::new(4, 10);

		{
			let mut page_4 = buffer.write_page(4);
			page_4.copy_from_slice(b"moin");
		}

		{
			let mut page_5 = buffer.write_page(5);
			page_5.copy_from_slice(b"tree");
		}

		let page_4 = buffer.read_page(4);
		let page_5 = buffer.read_page(5);

		assert_eq!(*page_4, *b"moin");
		assert_eq!(*page_5, *b"tree");
	}
}
