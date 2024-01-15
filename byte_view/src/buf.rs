use std::ops::{Deref, DerefMut};
use std::slice;
use std::{alloc::Layout, marker::PhantomData, ptr::NonNull};

use std::alloc::{alloc, dealloc, handle_alloc_error, realloc};

use thiserror::Error;

use crate::ByteView;

#[derive(Debug, Error)]
pub enum BufError {
	#[error("Requested buffer size {size} is too small; type requires at least {min}")]
	TooSmall { size: usize, min: usize },
}

#[derive(Debug)]
pub struct ViewBuf<T: ?Sized + ByteView> {
	size: usize,
	bytes: NonNull<u8>,
	marker: PhantomData<T>,
}

// Safety: ViewBuf's internal `bytes` pointer is unique
unsafe impl<T: ?Sized + ByteView> Send for ViewBuf<T> {}

// Safety: ViewBuf does not provide interior mutability
unsafe impl<T: ?Sized + ByteView> Sync for ViewBuf<T> {}

impl<T: ?Sized + ByteView> ViewBuf<T> {
	pub fn new() -> Self {
		unsafe { Self::new_with_size_unchecked(T::MIN_SIZE) }
	}

	pub fn new_with_size(size: usize) -> Result<Self, BufError> {
		if size < T::MIN_SIZE {
			return Err(BufError::TooSmall {
				size,
				min: T::MIN_SIZE,
			});
		}
		unsafe { Ok(Self::new_with_size_unchecked(size)) }
	}

	/// # Safety
	///
	/// This function may only be called if `size` is at least the minimum size
	/// for `T`
	pub unsafe fn new_with_size_unchecked(size: usize) -> Self {
		let layout = Self::layout_for(size);
		let Some(bytes) = NonNull::new(alloc(layout)) else {
			handle_alloc_error(layout);
		};

		let mut buf = Self {
			size,
			bytes,
			marker: PhantomData,
		};
		buf.as_bytes_mut().fill(0);
		buf
	}

	pub fn resize(&mut self, new_size: usize) -> Result<(), BufError> {
		if new_size < T::MIN_SIZE {
			return Err(BufError::TooSmall {
				size: new_size,
				min: T::MIN_SIZE,
			});
		}
		unsafe { self.resize_unchecked(new_size) };
		Ok(())
	}

	/// # Safety
	///
	/// This function may only be called if `size` is at least the minimum size
	/// for `T`
	pub unsafe fn resize_unchecked(&mut self, new_size: usize) {
		let Some(new_bytes) =
			NonNull::new(unsafe { realloc(self.bytes.as_ptr(), self.layout(), new_size) })
		else {
			handle_alloc_error(Self::layout_for(new_size))
		};
		if new_size < self.size {
			let new_bytes_start = self.size;
			self.as_bytes_mut()[new_bytes_start..].fill(0);
		}
		self.size = new_size;
		self.bytes = new_bytes;
	}

	#[inline]
	pub fn as_bytes(&self) -> &[u8] {
		unsafe { slice::from_raw_parts(self.bytes.as_ptr(), self.size) }
	}

	#[inline]
	pub fn as_bytes_mut(&mut self) -> &mut [u8] {
		unsafe { slice::from_raw_parts_mut(self.bytes.as_ptr(), self.size) }
	}

	#[inline]
	pub fn size(&self) -> usize {
		self.size
	}

	#[inline]
	fn layout(&self) -> Layout {
		Self::layout_for(self.size)
	}

	#[inline]
	fn layout_for(size: usize) -> Layout {
		Layout::from_size_align(size, T::ALIGN).expect("Failed to construct ByteView layout")
	}
}

impl<T: ?Sized + ByteView> Drop for ViewBuf<T> {
	fn drop(&mut self) {
		unsafe { dealloc(self.bytes.as_ptr(), self.layout()) }
	}
}

impl<T: ?Sized + ByteView> Default for ViewBuf<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: ?Sized + ByteView> Deref for ViewBuf<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		unsafe { T::from_bytes_unchecked(self.as_bytes()) }
	}
}

impl<T: ?Sized + ByteView> DerefMut for ViewBuf<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		unsafe { T::from_bytes_mut_unchecked(self.as_bytes_mut()) }
	}
}

impl<T: ?Sized + ByteView> AsRef<T> for ViewBuf<T> {
	fn as_ref(&self) -> &T {
		self
	}
}

impl<T: ?Sized + ByteView> AsMut<T> for ViewBuf<T> {
	fn as_mut(&mut self) -> &mut T {
		self
	}
}

impl<T: ByteView> From<T> for ViewBuf<T> {
	fn from(value: T) -> Self {
		let mut buf = Self::new();
		*buf = value;
		buf
	}
}

#[cfg(test)]
mod tests {
	use std::mem::size_of;

	use super::*;

	#[test]
	fn sized_type() {
		let mut buf: ViewBuf<u64> = ViewBuf::new();
		assert_eq!(buf.size(), size_of::<u64>());

		*buf = 25;
		assert_eq!(*buf, 25);
		assert_eq!(buf.as_bytes(), 25_u64.to_ne_bytes());
	}

	#[test]
	fn unsized_type() {
		let mut buf: ViewBuf<[u32]> = ViewBuf::new_with_size(3 * size_of::<u32>()).unwrap();
		assert_eq!(buf.size(), 3 * size_of::<u32>());

		buf[0] = 25;
		buf[1] = 69;
		buf[2] = 420;
		assert_eq!(buf.deref().len(), 3);
		assert_eq!(*buf, [25, 69, 420]);
		assert_eq!(
			buf.as_bytes(),
			[
				25_u32.to_ne_bytes(),
				69_u32.to_ne_bytes(),
				420_u32.to_ne_bytes()
			]
			.concat()
		)
	}

	#[test]
	fn resize_unsized() {
		let mut buf: ViewBuf<[u32]> = ViewBuf::new_with_size(2 * size_of::<u32>()).unwrap();
		assert_eq!(buf.deref().len(), 2);

		buf[0] = 1;
		buf[1] = 2;

		buf.resize(3 * size_of::<u32>()).unwrap();
		assert_eq!(buf.deref().len(), 3);

		buf[2] = 3;

		assert_eq!(*buf, [1, 2, 3]);
	}

	#[test]
	fn init_to_zero() {
		let buf: ViewBuf<u64> = ViewBuf::new();

		assert_eq!(*buf, 0);
	}
}
