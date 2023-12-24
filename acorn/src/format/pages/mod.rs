use std::alloc::Layout;
use std::mem::{align_of, size_of};
use std::{ptr, usize};

use crate::storage::StorageFile;
use crate::utils::byte_view::ByteView;

mod freelist;
mod header;

pub use freelist::*;
pub use header::*;

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

#[repr(C)]
pub struct Page<H, I>
where
	H: ByteView,
	I: ByteView,
{
	pub header: H,
	pub items: [I],
}

impl<H, I> Page<H, I>
where
	H: ByteView,
	I: ByteView,
{
	#[inline]
	pub fn from_bytes(bytes: &[u8]) -> &Self {
		unsafe { &*ptr::from_raw_parts(bytes.as_ptr() as *const (), Self::num_items(bytes.len())) }
	}

	#[inline]
	pub fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
		unsafe {
			&mut *ptr::from_raw_parts_mut(
				bytes.as_mut_ptr() as *mut (),
				Self::num_items(bytes.len()),
			)
		}
	}

	const HEADER_PADDING: usize = Layout::new::<H>().padding_needed_for(align_of::<I>());
	const ITEM_PADDING: usize = Layout::new::<I>().padding_needed_for(align_of::<I>());

	const HEADER_SIZE_PADDED: usize = size_of::<H>() + Self::HEADER_PADDING;
	const ITEM_SIZE_PADDED: usize = size_of::<I>() + Self::ITEM_PADDING;

	#[inline]
	fn num_items(size: usize) -> usize {
		(size - Self::HEADER_SIZE_PADDED) / Self::ITEM_SIZE_PADDED
	}
}

#[cfg(test)]
mod tests {
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

	#[test]
	fn interpret_page() {
		let mut page: [u8; 9] = Default::default();
		page[0..4].copy_from_slice(&16_u32.to_ne_bytes());
		page[4..6].copy_from_slice(&69_u16.to_ne_bytes());
		page[6..8].copy_from_slice(&420_u16.to_ne_bytes());

		let test_page = Page::<u32, u16>::from_bytes(&page);
		assert_eq!(test_page.header, 16);
		assert_eq!(test_page.items.len(), 2);
		assert_eq!(test_page.items[0], 69);
		assert_eq!(test_page.items[1], 420);
	}
}
