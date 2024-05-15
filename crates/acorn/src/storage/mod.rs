use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use thiserror::Error;

#[cfg(test)]
use mockall::automock;

use crate::files::segment::PAGE_BODY_SIZE;
use crate::files::DatabaseFolder;
use crate::files::FileError;

pub(crate) use crate::files::PageId;
use crate::files::TransactionState;
use crate::files::WalIndex;

use cache::{PageCache, PageCacheApi, PageCacheConfig};
use physical::{PhysicalStorage, PhysicalStorageApi, PhysicalStorageConfig};

use wal::{Wal, WalApi, WalConfig};

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

	fn load_into_cache(&self, page_id: PageId) -> Result<PC::WriteGuard<'_>, StorageError> {
		let mut guard = self.cache.store(page_id);
		if let Err(error) = self.physical.read(page_id, &mut guard) {
			self.cache.scrap(page_id);
			return Err(error);
		}
		Ok(guard)
	}
}

#[cfg_attr(test, automock(
    type ReadGuard<'a> = &'a [u8];
    type WriteGuard<'a> = &'a mut [u8];
))]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait PageStorageApi {
	type ReadGuard<'a>: Deref<Target = [u8]> + 'a
	where
		Self: 'a;
	type WriteGuard<'a>: Deref<Target = [u8]> + DerefMut + 'a
	where
		Self: 'a;

	fn recover(&self) -> Result<(), StorageError>;
	fn read<'a>(&'a self, page_id: PageId) -> Result<Self::ReadGuard<'a>, StorageError>;
}

impl<PS, PC, W> PageStorageApi for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	type ReadGuard<'a> = PC::ReadGuard<'a> where Self: 'a;
	type WriteGuard<'a> = PC::WriteGuard<'a> where Self: 'a;

	fn recover(&self) -> Result<(), StorageError> {
		self.wal.recover(|write_op| {
			// TODO: This can probably be abstracted to use the page cache somehow
			let mut page = [0; PAGE_BODY_SIZE];
			self.physical.read(write_op.page_id, &mut page)?;
			page[write_op.offset as usize..write_op.offset as usize + write_op.buf.len()]
				.copy_from_slice(write_op.buf);
			self.physical
				.write(write_op.page_id, write_op.buf, write_op.index)?;
			Ok(())
		})
	}

	fn read(&self, page_id: PageId) -> Result<Self::ReadGuard<'_>, StorageError> {
		let guard = self.load_into_cache(page_id)?;
		Ok(self.cache.downgrade_guard(guard))
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_id;
	pub(super) use crate::files::test_helpers::wal_index;
}
