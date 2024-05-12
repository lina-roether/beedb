use thiserror::Error;

use crate::files::FileError;

mod cache;
mod physical;
mod wal;

pub(crate) use crate::files::PageId;
use crate::files::TransactionState;
use crate::files::WalIndex;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
	#[error("The WAL was never initialized!")]
	WalNotInitialized,

	#[error(transparent)]
	File(#[from] FileError),
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_id;
	pub(super) use crate::files::test_helpers::wal_index;
}
