use std::num::NonZeroU16;

use crate::utils::byte_view::ByteView;

#[repr(C)]
pub struct HeaderPage {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size: u16,
	pub num_pages: u16,
	pub freelist_trunk: Option<NonZeroU16>,
}

// Safety: No fields in HeaderPage have internal invariants
unsafe impl ByteView for HeaderPage {}

#[cfg(test)]
mod tests {
	use crate::utils::{byte_order::ByteOrder, byte_view::AlignedBytes};

	use super::*;

	#[test]
	fn read_header_page() {
		let mut bytes: AlignedBytes<12> = Default::default();
		bytes[0..4].copy_from_slice(b"TOME");
		bytes[4] = 1;
		bytes[5] = ByteOrder::Little as u8;
		bytes[6..8].copy_from_slice(&30000_u16.to_ne_bytes());
		bytes[8..10].copy_from_slice(&69_u16.to_ne_bytes());
		bytes[10..12].copy_from_slice(&3_u16.to_ne_bytes());

		let header_page = HeaderPage::from_bytes(bytes.as_ref());
		assert_eq!(header_page.magic, *b"TOME");
		assert_eq!(header_page.format_version, 1);
		assert_eq!(header_page.byte_order, ByteOrder::Little as u8);
		assert_eq!(header_page.page_size, 30000);
		assert_eq!(header_page.num_pages, 69);
		assert_eq!(header_page.freelist_trunk, NonZeroU16::new(3));
	}
}
