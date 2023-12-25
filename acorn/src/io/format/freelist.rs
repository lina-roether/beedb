use std::num::NonZeroU32;

use crate::utils::byte_view::ByteView;

use super::Page;

#[repr(C)]
pub struct FreelistPageHeader {
	pub next: Option<NonZeroU32>,
	pub length: u32,
}

unsafe impl ByteView for FreelistPageHeader {}

pub type FreelistPage = Page<FreelistPageHeader, Option<NonZeroU32>>;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn read_freelist_page() {
		let mut bytes = Vec::new();
		bytes.extend(0_u32.to_ne_bytes());
		bytes.extend(3_u32.to_ne_bytes());
		bytes.extend(1_u32.to_ne_bytes());
		bytes.extend(2_u32.to_ne_bytes());
		bytes.extend(0_u32.to_ne_bytes());
		bytes.extend([0x00, 0x00]);

		let page = FreelistPage::from_bytes(&bytes);

		assert_eq!(page.header.next, None);
		assert_eq!(page.header.length, 3);
		assert_eq!(page.items, [NonZeroU32::new(1), NonZeroU32::new(2), None])
	}

	#[test]
	fn write_freelist_page() {
		let mut bytes = Vec::new();
		bytes.extend(0_u32.to_ne_bytes());
		bytes.extend(0_u32.to_ne_bytes());
		bytes.extend(0_u32.to_ne_bytes());

		let page = FreelistPage::from_bytes_mut(&mut bytes);

		page.header.length = 1;
		page.items[0] = NonZeroU32::new(69);

		let mut expected = Vec::new();
		expected.extend(0_u32.to_ne_bytes());
		expected.extend(1_u32.to_ne_bytes());
		expected.extend(69_u32.to_ne_bytes());

		assert_eq!(bytes, expected);
	}
}
