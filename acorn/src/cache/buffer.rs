use std::{
	alloc::Layout,
	cell::UnsafeCell,
	iter,
	ops::{Deref, DerefMut, Range},
	sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use byte_view::{ByteView, Bytes};
use parking_lot::{lock_api::RawRwLock as _, Mutex, RawRwLock};
use static_assertions::assert_impl_all;

use crate::utils::aligned_buf::AlignedBuffer;

pub struct PageReadGuard<'a, T: ?Sized + ByteView> {
	lock: &'a RawRwLock,
	page: &'a Bytes<T>,
}

impl<'a, T: ?Sized + ByteView> Drop for PageReadGuard<'a, T> {
	fn drop(&mut self) {
		unsafe {
			self.lock.unlock_shared();
		}
	}
}

impl<'a, T: ?Sized + ByteView> Deref for PageReadGuard<'a, T> {
	type Target = Bytes<T>;

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a, T: ?Sized + ByteView> AsRef<Bytes<T>> for PageReadGuard<'a, T> {
	#[inline]
	fn as_ref(&self) -> &Bytes<T> {
		self
	}
}

pub struct PageWriteGuard<'a, T: ?Sized + ByteView> {
	lock: &'a RawRwLock,
	page: &'a mut Bytes<T>,
}

impl<'a, T: ?Sized + ByteView> Drop for PageWriteGuard<'a, T> {
	fn drop(&mut self) {
		unsafe {
			self.lock.unlock_exclusive();
		}
	}
}

impl<'a, T: ?Sized + ByteView> Deref for PageWriteGuard<'a, T> {
	type Target = Bytes<T>;

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.page
	}
}

impl<'a, T: ?Sized + ByteView> DerefMut for PageWriteGuard<'a, T> {
	#[inline]
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.page
	}
}

impl<'a, T: ?Sized + ByteView> AsRef<Bytes<T>> for PageWriteGuard<'a, T> {
	#[inline]
	fn as_ref(&self) -> &Bytes<T> {
		self
	}
}

impl<'a, T: ?Sized + ByteView> AsMut<Bytes<T>> for PageWriteGuard<'a, T> {
	#[inline]
	fn as_mut(&mut self) -> &mut Bytes<T> {
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
	pages: UnsafeCell<AlignedBuffer>,
}

// Safety: Each page in the buffer is effectively protected by an RwLock in its
// corresponding PageMeta. The area of the buffer belonging to the page is only
// read when a shared lock is acquired on it, and only written to when an
// exclusive lock is acquired on it.
unsafe impl Sync for PageBuffer {}

assert_impl_all!(PageBuffer: Send, Sync);

impl PageBuffer {
	const PAGE_ALIGNMENT: usize = 8;

	pub fn new(page_size: usize, length: usize) -> Self {
		let (buf_layout, page_size_padded) = Self::page_buffer_layout(page_size, length);

		Self {
			length,
			page_size,
			page_size_padded,
			meta: iter::repeat_with(PageMeta::default).take(length).collect(),
			freelist: Mutex::new(Vec::new()),
			last_filled: AtomicUsize::new(0),
			pages: UnsafeCell::new(AlignedBuffer::with_layout(buf_layout)),
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

	pub fn read_page<T: ?Sized + ByteView>(&self, index: usize) -> Option<PageReadGuard<T>> {
		let meta = &self.meta[index];
		if !meta.occupied.load(Ordering::Relaxed) {
			return None;
		};
		meta.lock.lock_shared();
		Some(PageReadGuard {
			lock: &meta.lock,
			page: Bytes::new(unsafe { &(*self.pages.get())[self.range_of_page(index)] }).unwrap(),
		})
	}

	pub fn write_page<T: ?Sized + ByteView>(&self, index: usize) -> Option<PageWriteGuard<T>> {
		let meta = &self.meta[index];
		if !meta.occupied.load(Ordering::Relaxed) {
			return None;
		};
		meta.lock.lock_exclusive();
		Some(PageWriteGuard {
			lock: &meta.lock,
			page: Bytes::new_mut(unsafe { &mut (*self.pages.get())[self.range_of_page(index)] })
				.unwrap(),
		})
	}

	fn range_of_page(&self, index: usize) -> Range<usize> {
		if index >= self.length {
			panic!(
				"Page buffer index {index} out of bounds for length {}",
				self.length
			);
		}
		let start = index * self.page_size_padded;
		let end = start + self.page_size;
		start..end
	}

	fn page_buffer_layout(page_size: usize, length: usize) -> (Layout, usize) {
		let page_layout = Layout::from_size_align(page_size, Self::PAGE_ALIGNMENT).unwrap();
		page_layout.repeat(length).unwrap()
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
			let mut page_1 = buffer.write_page::<[u8]>(idx_1).unwrap();
			page_1.copy_from_slice(b"moin");
		}

		{
			let mut page_2 = buffer.write_page::<[u8]>(idx_2).unwrap();
			page_2.copy_from_slice(b"tree");
		}

		let page_1 = buffer.read_page::<[u8]>(idx_1).unwrap();
		let page_2 = buffer.read_page::<[u8]>(idx_2).unwrap();

		assert_eq!(**page_1, *b"moin");
		assert_eq!(**page_2, *b"tree");
	}

	#[test]
	fn try_access_freed_index() {
		let buffer = PageBuffer::new(4, 10);

		let idx = buffer.allocate_page().unwrap();
		buffer.free_page(idx);

		assert!(buffer.read_page::<[u8]>(idx).is_none());
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
