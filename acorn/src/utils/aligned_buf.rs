use core::slice;
use std::{
	alloc::{alloc, dealloc, handle_alloc_error, realloc, Layout},
	fmt::Debug,
	ops::{Deref, DerefMut},
	ptr::{self},
};

use static_assertions::assert_impl_all;

pub struct AlignedBuffer {
	layout: Layout,
	bytes: *mut u8,
}

// Safety: AlignedBuffer's internal `bytes` pointer is unique
unsafe impl Send for AlignedBuffer {}

// Safety: AlignedBuffer requires a mutable reference for modification, so the
// borrow checker enforces soundness
unsafe impl Sync for AlignedBuffer {}

assert_impl_all!(AlignedBuffer: Send, Sync);

impl AlignedBuffer {
	pub fn new(align: usize) -> Self {
		let layout = Layout::from_size_align(0, align).unwrap();
		Self {
			layout,
			bytes: ptr::null_mut(),
		}
	}

	pub fn with_capacity(align: usize, cap: usize) -> Self {
		let mut buf = Self::new(align);
		buf.resize_to(cap);
		buf
	}

	pub fn with_layout(layout: Layout) -> Self {
		Self::with_capacity(layout.align(), layout.size())
	}

	pub fn for_type<T>() -> Self {
		Self::with_layout(Layout::new::<T>())
	}

	pub fn from_bytes(bytes: &[u8], align: usize) -> Self {
		let mut buf = Self::with_capacity(align, bytes.len());
		buf.copy_from_slice(bytes);
		buf
	}

	#[inline]
	pub fn align(&self) -> usize {
		self.layout.align()
	}

	pub fn clear(&mut self) {
		unsafe {
			dealloc(self.bytes, self.layout);
			self.set_cap(0);
		};
		self.bytes = ptr::null_mut();
	}

	#[inline]
	pub fn as_slice(&self) -> &[u8] {
		self
	}

	#[inline]
	pub fn as_slice_mut(&mut self) -> &mut [u8] {
		self
	}

	pub fn as_ptr(&self) -> *const u8 {
		self.bytes
	}

	pub fn as_mut_ptr(&mut self) -> *mut u8 {
		self.bytes
	}

	pub fn resize_to(&mut self, cap: usize) {
		if cap == 0 {
			self.clear();
		} else {
			unsafe { self.resize_allocation(cap) }
		}
	}

	unsafe fn resize_allocation(&mut self, cap: usize) {
		if self.bytes.is_null() {
			self.allocate(cap);
		} else {
			self.reallocate(cap);
		}
	}

	unsafe fn allocate(&mut self, cap: usize) {
		let layout = Layout::from_size_align(cap, self.align()).unwrap();
		let bytes = alloc(layout);
		if bytes.is_null() {
			handle_alloc_error(layout);
		}
		self.layout = layout;
		self.bytes = bytes;
	}

	unsafe fn reallocate(&mut self, cap: usize) {
		let new_bytes = realloc(self.bytes, self.layout, cap);
		self.layout = Layout::from_size_align(cap, self.align()).unwrap();
		if !new_bytes.is_null() {
			self.bytes = new_bytes;
		} else if self.bytes.is_null() {
			handle_alloc_error(self.layout);
		}
	}

	unsafe fn set_cap(&mut self, cap: usize) {
		self.layout = Layout::from_size_align(cap, self.align()).unwrap();
	}
}

impl Debug for AlignedBuffer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		(**self).fmt(f)
	}
}

impl PartialEq for AlignedBuffer {
	fn eq(&self, other: &Self) -> bool {
		**self == **other
	}
}

impl PartialEq<[u8]> for AlignedBuffer {
	fn eq(&self, other: &[u8]) -> bool {
		**self == *other
	}
}

impl Eq for AlignedBuffer {}

impl Deref for AlignedBuffer {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		if self.bytes.is_null() {
			return &[];
		}
		unsafe { slice::from_raw_parts(self.bytes, self.layout.size()) }
	}
}

impl DerefMut for AlignedBuffer {
	fn deref_mut(&mut self) -> &mut Self::Target {
		if self.bytes.is_null() {
			return &mut [];
		}
		unsafe { slice::from_raw_parts_mut(self.bytes, self.layout.size()) }
	}
}

impl AsRef<[u8]> for AlignedBuffer {
	fn as_ref(&self) -> &[u8] {
		self
	}
}

impl AsMut<[u8]> for AlignedBuffer {
	fn as_mut(&mut self) -> &mut [u8] {
		self
	}
}

impl Drop for AlignedBuffer {
	fn drop(&mut self) {
		self.clear();
	}
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn aligned_buffer() {
		let mut buffer = AlignedBuffer::with_capacity(8, 10);
		assert_eq!(buffer.len(), 10);
		buffer.fill(69);
		assert!(buffer.iter().all(|b| *b == 69));
		assert!(buffer.as_ptr().is_aligned_to(8));
	}

	#[test]
	fn resize_aligned_buffer() {
		let mut buffer = AlignedBuffer::with_capacity(8, 10);
		buffer.resize_to(69);
		assert_eq!(buffer.len(), 69);
	}
}
