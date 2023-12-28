use crate::{storage::PageNumber, utils::byte_view::ByteView};

#[repr(C)]
pub struct HeaderPage {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size_exponent: u8,
	pub num_pages: u16,
	pub freelist_trunk: Option<PageNumber>,
}

unsafe impl ByteView for HeaderPage {}

#[cfg(test)]
mod tests {
	use crate::utils::byte_order::ByteOrder;

	use super::*;

	#[test]
	fn read_header_page() {
		let mut bytes: Vec<u8> = Vec::new();
		bytes.extend(b"TOME");
		bytes.push(1);
		bytes.push(ByteOrder::Little as u8);
		bytes.push(3);
		bytes.push(0);
		bytes.extend(69_u16.to_ne_bytes());
		bytes.extend(3_u16.to_ne_bytes());

		let header_page = HeaderPage::from_bytes(&bytes);
		assert_eq!(header_page.magic, *b"TOME");
		assert_eq!(header_page.format_version, 1);
		assert_eq!(header_page.byte_order, ByteOrder::Little as u8);
		assert_eq!(header_page.page_size_exponent, 3);
		assert_eq!(header_page.num_pages, 69);
		assert_eq!(header_page.freelist_trunk, PageNumber::new(3));
	}
}
