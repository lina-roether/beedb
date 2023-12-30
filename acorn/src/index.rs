use core::fmt;
use std::num::NonZeroU16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
	pub segment_num: u32,
	pub page_num: NonZeroU16,
}

impl PageId {
	#[inline]
	pub fn new(segment_num: u32, page_num: NonZeroU16) -> Self {
		Self {
			segment_num,
			page_num,
		}
	}
}

impl fmt::Display for PageId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:08x}:{:04x}", self.segment_num, self.page_num)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageIndex {
	pub segment_num: u32,
	pub page_num: NonZeroU16,
	pub index: u16,
}

impl StorageIndex {
	#[inline]
	pub fn new(page_id: PageId, index: u16) -> Self {
		Self {
			segment_num: page_id.segment_num,
			page_num: page_id.page_num,
			index,
		}
	}

	#[inline]
	pub fn page_id(self) -> PageId {
		PageId::new(self.segment_num, self.page_num)
	}
}

impl fmt::Display for StorageIndex {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}:{:04x}", self.page_id(), self.index)
	}
}
