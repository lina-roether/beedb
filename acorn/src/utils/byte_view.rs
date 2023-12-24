use core::{panic, slice};
use std::{
	mem::size_of,
	num::{
		NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroU128, NonZeroU16,
		NonZeroU32, NonZeroU64, NonZeroU8,
	},
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
		unsafe { Self::from_bytes_unchecked(bytes) }
	}

	#[inline]
	fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
		if bytes.len() < size_of::<Self>() {
			panic!("Cannot use from_bytes_mut on a byte slice smaller than the target type")
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
