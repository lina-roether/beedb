use std::{ops::RangeInclusive, usize};

use thiserror::Error;

use crate::utils::units::*;

pub const SEGMENT_MAGIC: [u8; 4] = *b"ACNS";
pub const META_MAGIC: [u8; 4] = *b"ACNM";
pub const DEFAULT_PAGE_SIZE: usize = 16 * KiB;
pub const PAGE_SIZE_RANGE: RangeInclusive<usize> = (512 * B)..=(64 * KiB);
pub const SEGMENT_FORMAT_VERSION: u8 = 1;
pub const META_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Error)]
#[error(
	"Page size {0} is invalid; must be a power of two between {} and {}",
	display_size(*PAGE_SIZE_RANGE.start()),
	display_size(*PAGE_SIZE_RANGE.end())
)]
pub struct PageSizeBoundsError(usize);

#[inline]
pub fn validate_page_size(size: usize) -> Result<(), PageSizeBoundsError> {
	if !size.is_power_of_two() || !PAGE_SIZE_RANGE.contains(&size) {
		return Err(PageSizeBoundsError(size));
	}
	Ok(())
}
