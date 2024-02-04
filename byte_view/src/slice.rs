use std::{
	marker::PhantomData,
	mem::size_of,
	ops::{Deref, DerefMut},
	slice,
};

use thiserror::Error;

use crate::{transmute_unsized, transmute_unsized_mut, ByteView};

#[derive(Debug, Error)]
pub enum BytesError {
	#[error("Slice of bytes is too small ({size}); type requires at least {min}")]
	TooSmall { size: usize, min: usize },

	#[error("Slice of bytes does not have the required alignment ({0})")]
	Unaligned(usize),
}

#[derive(Debug)]
#[repr(transparent)]
pub struct ViewSlice<T: ?Sized + ByteView> {
	_marker: PhantomData<T>,
	bytes: [u8],
}

impl<T: ?Sized + ByteView> ViewSlice<T> {
	pub fn new(bytes: &[u8]) -> Result<&Self, BytesError> {
		if bytes.len() < T::MIN_SIZE {
			return Err(BytesError::TooSmall {
				size: bytes.len(),
				min: T::MIN_SIZE,
			});
		}
		if !bytes.as_ptr().is_aligned_to(T::ALIGN) {
			return Err(BytesError::Unaligned(T::ALIGN));
		}
		Ok(unsafe { transmute_unsized(bytes, bytes.len()) })
	}

	pub fn new_mut(bytes: &mut [u8]) -> Result<&mut Self, BytesError> {
		if bytes.len() < T::MIN_SIZE {
			return Err(BytesError::TooSmall {
				size: bytes.len(),
				min: T::MIN_SIZE,
			});
		}
		if !bytes.as_ptr().is_aligned_to(T::ALIGN) {
			return Err(BytesError::Unaligned(T::ALIGN));
		}
		Ok(unsafe { transmute_unsized_mut(bytes, bytes.len()) })
	}

	#[inline]
	pub fn as_bytes(&self) -> &[u8] {
		&self.bytes
	}

	#[inline]
	pub fn as_bytes_mut(&mut self) -> &mut [u8] {
		&mut self.bytes
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.bytes.len()
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.bytes.is_empty()
	}
}

impl<T: ?Sized + ByteView> Deref for ViewSlice<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		T::from_bytes(self.as_bytes())
	}
}

impl<T: ?Sized + ByteView> DerefMut for ViewSlice<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		T::from_bytes_mut(self.as_bytes_mut())
	}
}

impl<T: ByteView> ViewSlice<T> {
	pub fn from(value: &T) -> &Self {
		let byte_slice =
			unsafe { slice::from_raw_parts(value as *const T as *const u8, size_of::<T>()) };
		Self::new(byte_slice).unwrap()
	}

	pub fn from_mut(value: &mut T) -> &mut Self {
		let byte_slice =
			unsafe { slice::from_raw_parts_mut(value as *mut T as *mut u8, size_of::<T>()) };
		Self::new_mut(byte_slice).unwrap()
	}
}
