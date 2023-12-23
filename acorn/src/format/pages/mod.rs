mod freelist;

use core::slice;
use std::ptr::Pointee;
use std::{ptr, usize};

pub use freelist::*;

#[inline]
fn num_items_from_byte_length<P: ?Sized + Page>(len: usize) -> usize {
	debug_assert!(len >= P::HEADER_SIZE);
	(len - P::HEADER_SIZE) / P::ITEM_SIZE
}

#[inline]
fn byte_length_from_num_items<P: ?Sized + Page>(num_items: usize) -> usize {
	num_items * P::ITEM_SIZE + P::HEADER_SIZE
}

pub trait Page: Pointee<Metadata = usize> {
	const HEADER_SIZE: usize;
	const ITEM_SIZE: usize;

	#[inline]
	fn new(bytes: &[u8]) -> &Self {
		unsafe {
			&*ptr::from_raw_parts(
				bytes.as_ptr() as *const (),
				num_items_from_byte_length::<Self>(bytes.len()),
			)
		}
	}

	#[inline]
	fn new_mut(bytes: &mut [u8]) -> &mut Self {
		unsafe {
			&mut *ptr::from_raw_parts_mut(
				bytes.as_ptr() as *mut (),
				num_items_from_byte_length::<Self>(bytes.len()),
			)
		}
	}

	#[inline]
	fn as_bytes(&self) -> &[u8] {
		unsafe {
			slice::from_raw_parts(
				self as *const Self as *const u8,
				byte_length_from_num_items::<Self>(ptr::metadata(self as *const Self)),
			)
		}
	}

	#[inline]
	fn as_bytes_mut(&mut self) -> &[u8] {
		unsafe {
			slice::from_raw_parts_mut(
				self as *mut Self as *mut u8,
				byte_length_from_num_items::<Self>(ptr::metadata(self as *const Self)),
			)
		}
	}
}
