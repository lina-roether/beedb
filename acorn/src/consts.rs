use std::{ops::RangeInclusive, usize};

use thiserror::Error;

use crate::utils::units::*;

pub const SEGMENT_MAGIC: [u8; 4] = *b"ACNS";
pub const META_MAGIC: [u8; 4] = *b"ACNM";
pub const WAL_MAGIC: [u8; 4] = *b"ACNL";
pub const DEFAULT_PAGE_SIZE: u16 = 16 * KiB as u16;
pub const PAGE_SIZE_RANGE: RangeInclusive<u16> = (512 * B as u16)..=(32 * KiB as u16);
pub const SEGMENT_FORMAT_VERSION: u8 = 1;
pub const META_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Error)]
#[error(
	"Page size {0} is invalid; must be a power of two between {} and {}",
	display_size(*PAGE_SIZE_RANGE.start() as usize),
	display_size(*PAGE_SIZE_RANGE.end() as usize)
)]
pub struct PageSizeBoundsError(u16);

#[inline]
pub fn validate_page_size(size: u16) -> Result<(), PageSizeBoundsError> {
	if !size.is_power_of_two() || !PAGE_SIZE_RANGE.contains(&size) {
		return Err(PageSizeBoundsError(size));
	}
	Ok(())
}
