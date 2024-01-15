use std::{
	mem::size_of,
	ptr::{self, Pointee},
	slice,
};

use crate::ByteView;

/// # Safety
///
/// This function is only safe if T satisfies the safety requirements
/// of implementing ByteView and calling from_bytes_unchecked on bytes
#[inline]
pub unsafe fn transmute<T>(bytes: &[u8]) -> &T {
	&*(bytes.as_ptr() as *const () as *const T)
}

/// # Safety
///
/// This function is only safe if T satisfies the safety requirements
/// of implementing ByteView and calling from_bytes_mut_unchecked on bytes
#[inline]
pub unsafe fn transmute_mut<T>(bytes: &mut [u8]) -> &mut T {
	&mut *(bytes.as_mut_ptr() as *mut () as *mut T)
}

/// # Safety
///
/// This function is only safe if T satisfies the safety requirements
/// of implementing ByteView and calling from_bytes_unchecked on bytes, and
/// meta satisfies the safety constraints of std::ptr::from_raw_parts.
#[inline]
pub unsafe fn transmute_unsized<T: ?Sized>(bytes: &[u8], meta: <T as Pointee>::Metadata) -> &T {
	&*ptr::from_raw_parts(bytes.as_ptr() as *const (), meta)
}

/// # Safety
///
/// This function is only safe if T satisfies the safety requirements
/// of implementing ByteView and calling from_bytes_unchecked_mut on bytes, and
/// meta satisfies the safety constraints of std::ptr::from_raw_parts_mut.
#[inline]
pub unsafe fn transmute_unsized_mut<T: ?Sized>(
	bytes: &mut [u8],
	meta: <T as Pointee>::Metadata,
) -> &mut T {
	&mut *ptr::from_raw_parts_mut(bytes.as_mut_ptr() as *mut (), meta)
}

pub fn to_bytes<T>(value: &T) -> &[u8] {
	unsafe { slice::from_raw_parts(value as *const T as *const u8, size_of::<T>()) }
}

pub fn to_bytes_mut<T: ByteView>(value: &mut T) -> &mut [u8] {
	unsafe { slice::from_raw_parts_mut(value as *mut T as *mut u8, size_of::<T>()) }
}
