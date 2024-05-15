use std::collections::hash_map::Entry;
use std::collections::HashMap;
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

use self::cache::PageWriteGuard;

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

pub(crate) struct Transaction<'a, PS = PhysicalStorage, PC = PageCache, W = Wal>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	id: u64,
	locks: HashMap<PageId, PC::WriteGuard<'a>>,
	storage: &'a PageStorage<PS, PC, W>,
}

impl<'a, PS, PC, W> Transaction<'a, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn new(id: u64, storage: &'a PageStorage<PS, PC, W>) -> Self {
		Self {
			id,
			storage,
			locks: HashMap::new(),
		}
	}

	fn acquire_lock(&mut self, page_id: PageId) -> Result<(), StorageError> {
		if let Entry::Vacant(e) = self.locks.entry(page_id) {
			let guard = self.storage.load_into_cache(page_id)?;
			e.insert(guard);
		}
		Ok(())
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait TransactionApi {
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError>;
	fn commit(self) -> Result<(), StorageError>;
	fn undo(self) -> Result<(), StorageError>;
}

impl<'a, PS, PC, W> TransactionApi for Transaction<'a, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		if let Some(guard) = self.locks.get(&page_id) {
			buf.copy_from_slice(&guard[offset..offset + buf.len()]);
		} else {
			buf.copy_from_slice(&self.storage.read(page_id)?[offset..offset + buf.len()]);
		}
		Ok(())
	}

	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		self.acquire_lock(page_id)?;
		let guard = self.locks.get_mut(&page_id).unwrap();
		let mut from: Box<[u8]> = vec![0; buf.len()].into();
		from.copy_from_slice(&guard[offset..offset + buf.len()]);

		self.storage.wal.log_write(wal::WriteLog {
			transaction_id: self.id,
			page_id,
			offset: u16::try_from(offset).expect("Write offset must be 16-bit!"),
			from: &from,
			to: buf,
		})?;

		guard[offset..offset + buf.len()].copy_from_slice(buf);
		Ok(())
	}

	fn commit(self) -> Result<(), StorageError> {
		self.storage.wal.log_commit(wal::CommitLog {
			transaction_id: self.id,
		})?;
		Ok(())
	}

	fn undo(self) -> Result<(), StorageError> {
		self.storage.wal.undo(self.id, |write_op| {
			let Some(guard) = self.locks.get(&write_op.page_id) else {
				panic!("An undo operation tried to undo a write to a page that the transaction did not access!");
			};
			todo!("Need to remember the WAL index for write operations");
			Ok(())
		})
	}
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
