use std::num::NonZeroU16;

use crate::utils::byte_view::ByteView;

use super::Page;

#[repr(C)]
pub struct FreelistPageHeader {
	pub next: Option<NonZeroU16>,
	pub length: u16,
}

unsafe impl ByteView for FreelistPageHeader {}

pub type FreelistPage = Page<FreelistPageHeader, Option<NonZeroU16>>;

#[cfg(test)]
mod tests {
	use crate::utils::byte_view::AlignedBytes;

	use super::*;

	#[test]
	fn read_freelist_page() {
		let mut bytes: AlignedBytes<11> = Default::default();
		bytes[0..2].copy_from_slice(&0_u16.to_ne_bytes());
		bytes[2..4].copy_from_slice(&3_u16.to_ne_bytes());
		bytes[4..6].copy_from_slice(&1_u16.to_ne_bytes());
		bytes[6..8].copy_from_slice(&2_u16.to_ne_bytes());
		bytes[8..10].copy_from_slice(&0_u16.to_ne_bytes());
		bytes[10] = 0x00;

		let page = FreelistPage::from_bytes(bytes.as_ref());

		assert_eq!(page.header.next, None);
		assert_eq!(page.header.length, 3);
		assert_eq!(page.items, [NonZeroU16::new(1), NonZeroU16::new(2), None])
	}

	#[test]
	fn write_freelist_page() {
		let mut bytes: AlignedBytes<6> = Default::default();

		let page = FreelistPage::from_bytes_mut(bytes.as_mut());

		page.header.length = 1;
		page.items[0] = NonZeroU16::new(69);

		let mut expected = Vec::new();
		expected.extend(0_u16.to_ne_bytes());
		expected.extend(1_u16.to_ne_bytes());
		expected.extend(69_u16.to_ne_bytes());

		assert_eq!(bytes.as_slice(), expected);
	}
}
