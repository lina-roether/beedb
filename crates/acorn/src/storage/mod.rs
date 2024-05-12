use std::sync::Arc;

use thiserror::Error;

#[cfg(test)]
use mockall::automock;

use crate::files::segment::PAGE_BODY_SIZE;
use crate::files::DatabaseFolder;
use crate::files::FileError;

mod cache;
mod physical;
mod wal;

pub(crate) use crate::files::PageId;
use crate::files::TransactionState;
use crate::files::WalIndex;

use cache::{PageCache, PageCacheApi, PageCacheConfig};
use physical::{PhysicalStorage, PhysicalStorageApi, PhysicalStorageConfig};
use wal::{Wal, WalApi, WalConfig};

#[derive(Debug, Error)]
pub(crate) enum StorageError {
	#[error("The WAL was never initialized!")]
	WalNotInitialized,

	#[error(transparent)]
	File(#[from] FileError),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PageStorageConfig {
	pub physical_storage: PhysicalStorageConfig,
	pub page_cache: PageCacheConfig,
	pub wal: WalConfig,
}

#[derive(Debug)]
pub(crate) struct PageStorage<PS = PhysicalStorage, PC = PageCache, W = Wal>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	physical: PS,
	cache: PC,
	wal: W,
}

impl PageStorage {
	pub fn create(
		folder: Arc<DatabaseFolder>,
		config: &PageStorageConfig,
	) -> Result<Self, StorageError> {
		Ok(Self::new(
			PhysicalStorage::new(Arc::clone(&folder), &config.physical_storage),
			PageCache::new(&config.page_cache),
			Wal::create(Arc::clone(&folder), &config.wal)?,
		))
	}

	pub fn open(
		folder: Arc<DatabaseFolder>,
		config: &PageStorageConfig,
	) -> Result<Self, StorageError> {
		Ok(Self::new(
			PhysicalStorage::new(Arc::clone(&folder), &config.physical_storage),
			PageCache::new(&config.page_cache),
			Wal::open(Arc::clone(&folder), &config.wal)?,
		))
	}
}

impl<PS, PC, W> PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn new(physical: PS, cache: PC, wal: W) -> Self {
		Self {
			physical,
			cache,
			wal,
		}
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait PageStorageApi {
	fn recover(&self) -> Result<(), StorageError>;
}

impl<PS, PC, W> PageStorageApi for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn recover(&self) -> Result<(), StorageError> {
		self.wal.recover(|write_op| {
			let mut page = [0; PAGE_BODY_SIZE];
			self.physical.read(write_op.page_id, &mut page)?;
			page[write_op.offset as usize..write_op.offset as usize + write_op.buf.len()]
				.copy_from_slice(write_op.buf);
			self.physical
				.write(write_op.page_id, write_op.buf, write_op.index)?;
			Ok(())
		})
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_id;
	pub(super) use crate::files::test_helpers::wal_index;
}
