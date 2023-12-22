use std::{num::NonZeroU32, usize};

/*
 * Freelist page format
 *
 * - 4 bytes: Next freelist page or 0
 * - 4 bytes: Number of entries in the page ()
 * - n * 4 bytes: Free page numbers
 */

pub struct FreelistPage {
	next_freelist_page: Option<NonZeroU32>,
	capacity: usize,
	length: usize,
	free_pages: Box<[Option<NonZeroU32>]>,
}

impl FreelistPage {
	pub fn from_bytes(bytes: &[u8]) -> Self {
		let next_freelist_page =
			NonZeroU32::new(u32::from_be_bytes(bytes[0..4].try_into().unwrap()));
		let length = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
		let cap = bytes.len() / 4 - 2;
		let mut free_pages = vec![None; cap].into_boxed_slice();

		for i in 0..usize::min(length, cap) {
			let offset = i * 4 + 8;
			free_pages[i] = NonZeroU32::new(u32::from_be_bytes(
				bytes[offset..offset + 4].try_into().unwrap(),
			));
		}

		Self {
			next_freelist_page,
			capacity: cap,
			length,
			free_pages,
		}
	}

	#[must_use]
	pub fn push_free_page(&mut self, page: NonZeroU32) -> bool {
		if self.is_full() {
			return false;
		}
		self.free_pages[self.length] = Some(page);
		true
	}

	#[inline]
	fn capacity(&self) -> usize {
		self.capacity
	}

	#[inline]
	fn length(&self) -> usize {
		self.length
	}

	#[inline]
	fn is_full(&self) -> bool {
		self.length >= self.capacity
	}

	#[inline]
	fn is_empty(&self) -> bool {
		self.length == 0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_freelist_page() {
		let data = [
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
			0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00,
		];

		let freelist_page = FreelistPage::from_bytes(&data);
		assert_eq!(freelist_page.next_freelist_page, None);
		assert_eq!(freelist_page.length, 4);
		assert_eq!(
			freelist_page.free_pages,
			vec![
				Some(NonZeroU32::new(1).unwrap()),
				Some(NonZeroU32::new(2).unwrap()),
				Some(NonZeroU32::new(3).unwrap()),
				Some(NonZeroU32::new(4).unwrap()),
				None
			]
			.into_boxed_slice()
		)
	}
}
