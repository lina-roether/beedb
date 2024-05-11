use std::num::{NonZeroU16, NonZeroU64};

use thiserror::Error;

use crate::files::FileError;

mod cache;
mod physical;
mod wal;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
	#[error("The WAL was never initialized!")]
	WalNotInitialized,

	#[error(transparent)]
	File(#[from] FileError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionState {
	pub first_gen: u64,
	pub last_index: WalIndex,
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
	pub offset: NonZeroU64,
}

impl WalIndex {
	pub fn new(generation: u64, offset: NonZeroU64) -> Self {
		Self { generation, offset }
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	macro_rules! page_id {
		($segment:expr, $page:expr) => {
			$crate::storage::PageId::new($segment, std::num::NonZeroU16::new($page).unwrap())
		};
	}
	pub(crate) use page_id;

	macro_rules! wal_index {
		($gen:expr, $offset:expr) => {
			$crate::storage::WalIndex::new($gen, std::num::NonZeroU64::new($offset).unwrap())
		};
	}
	pub(crate) use wal_index;
}
