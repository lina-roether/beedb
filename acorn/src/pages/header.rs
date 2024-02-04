use std::num::NonZeroU16;

use byte_view::{ByteView, ViewBuf};

use super::WriteOp;

#[derive(Debug, ByteView)]
#[repr(C)]
pub(crate) struct HeaderPage {
	pub magic: [u8; 4],
	pub format_version: u8,
	pub byte_order: u8,
	pub page_size: u16,
	pub num_pages: u16,
	pub freelist_trunk: Option<NonZeroU16>,
}

impl HeaderPage {
	pub(crate) fn write(page: &ViewBuf<Self>) -> WriteOp {
		WriteOp::new(0, page.as_bytes())
	}
}
