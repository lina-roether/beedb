use core::{panic, slice};
use std::{
	alloc::{alloc, dealloc, handle_alloc_error, realloc, Layout},
	mem::{align_of, size_of},
	num::{
		NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroU128, NonZeroU16,
		NonZeroU32, NonZeroU64, NonZeroU8,
	},
	ops::{Deref, DerefMut},
	ptr::{self},
};

/// This trait indicates that it is safe to reinterpret
/// a byte array as this type.
///
/// # Safety
///
/// This trait may only be implemented if **ANY** possible combination of bits
/// is a legal value for this type and doesn't violate any invariants.
pub unsafe trait ByteView: Sized {
	#[inline]
	fn from_bytes(bytes: &[u8]) -> &Self {
		if bytes.len() < size_of::<Self>() {
			panic!("Cannot use from_bytes on a byte slice smaller than the target type")
		}
		if !bytes.as_ptr().is_aligned_to(align_of::<Self>()) {
			panic!("Cannot use from_bytes on an unaligned pointer.")
		}
		unsafe { Self::from_bytes_unchecked(bytes) }
	}

	#[inline]
	fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
		if bytes.len() < size_of::<Self>() {
			panic!("Cannot use from_bytes_mut on a byte slice smaller than the target type")
		}
		if !bytes.as_ptr().is_aligned_to(align_of::<Self>()) {
			panic!("Cannot use from_bytes on an unaligned pointer.")
		}
		unsafe { Self::from_bytes_mut_unchecked(bytes) }
	}

	#[inline]
	fn as_bytes(&self) -> &[u8] {
		unsafe { slice::from_raw_parts(self as *const Self as *const u8, size_of::<Self>()) }
	}

	#[inline]
	fn as_bytes_mut(&mut self) -> &mut [u8] {
		unsafe { slice::from_raw_parts_mut(self as *mut Self as *mut u8, size_of::<Self>()) }
	}

	#[inline]
	unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
		unsafe { &*(bytes.as_ptr() as *const () as *const Self) }
	}

	#[inline]
	unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
		unsafe { &mut *(bytes.as_mut_ptr() as *mut () as *mut Self) }
	}
}

macro_rules! impl_byte_view {
    ($($ty:ty),*) => {
        $(
            unsafe impl ByteView for $ty {}
         )*
    };
}

impl_byte_view!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128);
impl_byte_view!(
	Option<NonZeroU8>,
	Option<NonZeroI8>,
	Option<NonZeroU16>,
	Option<NonZeroI16>,
	Option<NonZeroU32>,
	Option<NonZeroI32>,
	Option<NonZeroU64>,
	Option<NonZeroI64>,
	Option<NonZeroU128>,
	Option<NonZeroI128>
);

#[repr(align(8))]
pub struct AlignedBytes<const S: usize>([u8; S]);

impl<const S: usize> Deref for AlignedBytes<S> {
	type Target = [u8; S];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<const S: usize> DerefMut for AlignedBytes<S> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl<const S: usize> From<[u8; S]> for AlignedBytes<S> {
	fn from(value: [u8; S]) -> Self {
		Self(value)
	}
}

impl<const S: usize> AsRef<[u8; S]> for AlignedBytes<S> {
	fn as_ref(&self) -> &[u8; S] {
		self
	}
}

impl<const S: usize> AsMut<[u8; S]> for AlignedBytes<S> {
	fn as_mut(&mut self) -> &mut [u8; S] {
		self
	}
}

impl<const S: usize> AsRef<[u8]> for AlignedBytes<S> {
	fn as_ref(&self) -> &[u8] {
		&self.0
	}
}

impl<const S: usize> AsMut<[u8]> for AlignedBytes<S> {
	fn as_mut(&mut self) -> &mut [u8] {
		&mut self.0
	}
}

impl<const S: usize> Default for AlignedBytes<S> {
	fn default() -> Self {
		Self([0; S])
	}
}

pub struct AlignedBuffer {
	layout: Layout,
	bytes: *mut u8,
}

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
	use std::mem::{self};

	use super::*;

	#[test]
	fn aligned_bytes() {
		let mut bytes: AlignedBytes<10> = Default::default();
		assert_eq!(bytes.len(), 10);
		bytes.fill(69);
		assert!(bytes.iter().all(|b| *b == 69));
		assert_eq!(mem::align_of_val(&bytes), 8);
	}

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
