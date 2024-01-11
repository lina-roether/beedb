use std::num::NonZeroU16;

use byte_view::ByteView;

#[derive(Debug, ByteView)]
#[repr(C)]
pub struct HeaderPage {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size: u16,
	pub num_pages: u16,
	pub free_pages: u16,
	pub freelist_trunk: Option<NonZeroU16>,
}
