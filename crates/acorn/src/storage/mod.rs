use std::num::NonZeroU16;

use thiserror::Error;

use crate::files::FileError;

mod physical;
mod wal;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
	#[error(transparent)]
	File(#[from] FileError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PageId {
	pub segment_num: u32,
	pub page_num: NonZeroU16,
}

impl PageId {
	pub fn new(segment_num: u32, page_num: NonZeroU16) -> Self {
		Self {
			segment_num,
			page_num,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WalIndex {
	pub generation: u64,
	pub offset: u64,
}

impl WalIndex {
	pub fn new(generation: u64, offset: u64) -> Self {
		Self { generation, offset }
	}
}
