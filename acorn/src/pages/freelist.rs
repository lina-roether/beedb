use std::{mem::size_of, num::NonZeroU16};

use byte_view::{ByteView, ViewBuf};

use super::WriteOp;

#[derive(Debug, ByteView)]
#[dynamically_sized]
#[repr(C)]
pub(crate) struct FreelistPage {
	pub next: Option<NonZeroU16>,
	pub length: u16,
	pub items: [Option<NonZeroU16>],
}

impl FreelistPage {
	const ITEM_SIZE: usize = size_of::<Option<NonZeroU16>>();

	pub fn write_header(page: &ViewBuf<Self>) -> WriteOp {
		WriteOp::new(0, &page.as_bytes()[0..Self::MIN_SIZE])
	}

	pub fn write_item(page: &ViewBuf<Self>, index: usize) -> WriteOp {
		let start = Self::MIN_SIZE + index * Self::ITEM_SIZE;
		let end = Self::MIN_SIZE + (index + 1) * Self::ITEM_SIZE;
		WriteOp::new(start, &page.as_bytes()[start..end])
	}
}
