use std::{
	marker::PhantomData,
	ops::{Deref, DerefMut},
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
pub struct Bytes<T: ?Sized + ByteView> {
	_marker: PhantomData<T>,
	bytes: [u8],
}

impl<T: ?Sized + ByteView> Bytes<T> {
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

impl<T: ?Sized + ByteView> Deref for Bytes<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		T::from_bytes(self.as_bytes())
	}
}

impl<T: ?Sized + ByteView> DerefMut for Bytes<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		T::from_bytes_mut(self.as_bytes_mut())
	}
}
