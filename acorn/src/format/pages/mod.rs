use core::slice;
use std::ptr::Pointee;
use std::{ptr, usize};

use crate::storage::StorageFile;

mod freelist;

pub use freelist::*;

use super::Error;

pub struct PageStorage<F: StorageFile> {
	page_size: usize,
	file: F,
}

impl<F: StorageFile> PageStorage<F> {
	#[inline]
	pub fn new(file: F, page_size: usize) -> Self {
		Self { page_size, file }
	}

	#[inline]
	pub fn page_size(&self) -> usize {
		self.page_size
	}

	#[inline]
	pub fn read_page(&self, buf: &mut [u8], page_number: u32) -> Result<(), Error> {
		self.file
			.read_at(&mut buf[0..self.page_size], self.page_offset(page_number))?;
		Ok(())
	}

	#[inline]
	pub fn write_page(&mut self, buf: &[u8], page_number: u32) -> Result<(), Error> {
		self.file
			.write_at(&buf[0..self.page_size], self.page_offset(page_number))?;
		Ok(())
	}

	#[inline]
	fn page_offset(&self, page_number: u32) -> u64 {
		(page_number as u64) * (self.page_size as u64)
	}
}

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

#[cfg(test)]
mod tests {
	use std::mem::size_of;

	use super::*;

	#[test]
	fn read_page() {
		let data = vec![0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04];
		let pages = PageStorage::new(data, 4);

		let mut buf: [u8; 4] = Default::default();
		pages.read_page(&mut buf, 1).unwrap();

		assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
	}

	#[test]
	fn write_page() {
		let mut pages = PageStorage::new(Vec::new(), 4);

		pages.write_page(&[0xaa, 0xbb, 0xcc, 0xdd], 2).unwrap();

		assert_eq!(
			pages.file,
			vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0xbb, 0xcc, 0xdd]
		);
	}

	struct TestPage {
		number: u32,
		items: [u16],
	}

	impl Page for TestPage {
		const HEADER_SIZE: usize = size_of::<u32>();
		const ITEM_SIZE: usize = size_of::<u16>();
	}

	#[test]
	fn interpret_page() {
		let mut page: [u8; 9] = Default::default();
		page[0..4].copy_from_slice(&16_u32.to_ne_bytes());
		page[4..6].copy_from_slice(&69_u16.to_ne_bytes());
		page[6..8].copy_from_slice(&420_u16.to_ne_bytes());

		let test_page = TestPage::new(&page);
		assert_eq!(test_page.number, 16);
		assert_eq!(test_page.items.len(), 2);
		assert_eq!(test_page.items[0], 69);
		assert_eq!(test_page.items[1], 420);
	}
}
