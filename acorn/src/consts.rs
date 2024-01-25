use std::{ops::RangeInclusive, usize};

use thiserror::Error;

use crate::utils::units::*;

pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"ACNS";
pub(crate) const META_MAGIC: [u8; 4] = *b"ACNM";
pub(crate) const WAL_MAGIC: [u8; 4] = *b"ACNL";
pub(crate) const DEFAULT_PAGE_SIZE: u16 = 16 * KiB as u16;
pub(crate) const PAGE_SIZE_RANGE: RangeInclusive<u16> = (512 * B as u16)..=(32 * KiB as u16);
pub(crate) const SEGMENT_FORMAT_VERSION: u8 = 1;
pub(crate) const META_FORMAT_VERSION: u8 = 1;
pub(crate) const PAGE_ALIGNMENT: usize = 8;

#[derive(Debug, Error)]
#[error(
	"Page size {0} is invalid; must be a power of two between {} and {}",
	display_size(*PAGE_SIZE_RANGE.start() as usize),
	display_size(*PAGE_SIZE_RANGE.end() as usize)
)]
pub(crate) struct PageSizeBoundsError(u16);

#[inline]
pub(crate) fn validate_page_size(size: u16) -> Result<(), PageSizeBoundsError> {
	if !size.is_power_of_two() || !PAGE_SIZE_RANGE.contains(&size) {
		return Err(PageSizeBoundsError(size));
	}
	Ok(())
}
