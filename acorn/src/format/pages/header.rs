use std::num::NonZeroU32;

use crate::utils::byte_order::ByteOrder;

pub struct HeaderPage {
	magic: [u8; 4],
	page_size_exponent: u8,
	byte_order: ByteOrder,
	num_pages: usize,
	freelist_trunk: Option<NonZeroU32>,
	freelist_length: usize,
}
