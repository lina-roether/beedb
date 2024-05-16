use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use thiserror::Error;

#[cfg(test)]
use mockall::automock;

#[cfg(test)]
use crate::storage::cache::{MockPageReadGuardApi, MockPageWriteGuardApi};

use crate::files::DatabaseFolder;
use crate::files::FileError;
use crate::storage::cache::PageWriteGuardApi;

pub(crate) use crate::files::PageId;
use crate::files::TransactionState;
use crate::files::WalIndex;

use cache::{PageCache, PageCacheApi, PageCacheConfig};
use physical::{PhysicalStorage, PhysicalStorageApi, PhysicalStorageConfig};

use wal::{Wal, WalApi, WalConfig};

use self::cache::PageReadGuardApi;
use self::physical::ReadOp;
use self::physical::WriteOp;

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
			guard.read(offset, buf);
		} else {
			self.storage.read_guard(page_id)?.read(offset, buf);
		}
		Ok(())
	}

	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		self.acquire_lock(page_id)?;
		let guard = self.locks.get_mut(&page_id).unwrap();
		let mut from: Box<[u8]> = vec![0; buf.len()].into();
		guard.read(0, &mut from);

		let wal_index = self.storage.wal.log_write(wal::WriteLog {
			transaction_id: self.id,
			page_id,
			offset: u16::try_from(offset).expect("Write offset must be 16-bit!"),
			from: &from,
			to: buf,
		})?;

		guard.write(offset, buf, wal_index);
		Ok(())
	}

	fn commit(self) -> Result<(), StorageError> {
		self.storage.wal.log_commit(wal::CommitLog {
			transaction_id: self.id,
		})?;
		Ok(())
	}

	fn undo(mut self) -> Result<(), StorageError> {
		self.storage.wal.undo(self.id, |write_op| {
			let Some(guard) = self.locks.get_mut(&write_op.page_id) else {
				panic!("An undo operation tried to undo a write to a page that the transaction did not access!");
			};
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
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
	transaction_counter: AtomicU64,
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
			transaction_counter: AtomicU64::new(0),
		}
	}

	fn load_into_cache(&self, page_id: PageId) -> Result<PC::WriteGuard<'_>, StorageError> {
		let mut guard = self.cache.store(page_id);
		if let Err(error) = self.physical.read(ReadOp {
			page_id,
			buf: guard.body_mut(),
		}) {
			self.cache.scrap(page_id);
			return Err(error);
		}
		Ok(guard)
	}

	fn read_guard(&self, page_id: PageId) -> Result<PC::ReadGuard<'_>, StorageError> {
		let guard = self.load_into_cache(page_id)?;
		Ok(self.cache.downgrade_guard(guard))
	}
}

#[cfg_attr(test, automock(
    type ReadGuard<'a> = MockPageReadGuardApi;
    type WriteGuard<'a> = MockPageWriteGuardApi;
    type Transaction<'a> = MockTransactionApi;
))]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait PageStorageApi {
	type ReadGuard<'a>: PageReadGuardApi + 'a
	where
		Self: 'a;
	type WriteGuard<'a>: PageWriteGuardApi + 'a
	where
		Self: 'a;
	type Transaction<'a>: TransactionApi + 'a
	where
		Self: 'a;

	fn recover(&self) -> Result<(), StorageError>;
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	fn transaction<'a>(&'a self) -> Self::Transaction<'a>;
}

impl<PS, PC, W> PageStorageApi for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	type ReadGuard<'a> = PC::ReadGuard<'a> where Self: 'a;
	type WriteGuard<'a> = PC::WriteGuard<'a> where Self: 'a;
	type Transaction<'a> = Transaction<'a, PS, PC, W> where Self: 'a;

	fn recover(&self) -> Result<(), StorageError> {
		self.wal.recover(&mut |write_op| {
			let mut guard = self.load_into_cache(write_op.page_id)?;
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
			self.physical.write(WriteOp {
				wal_index: write_op.index,
				page_id: write_op.page_id,
				buf: guard.body(),
			})?;
			Ok(())
		})
	}

	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		self.read_guard(page_id)?.read(offset, buf);
		Ok(())
	}

	fn transaction(&self) -> Transaction<'_, PS, PC, W> {
		let transaction_id = self.transaction_counter.load(Ordering::Acquire);
		// FIXME: This can theoretically lead to duplicate transaction ids.
		self.transaction_counter
			.store(transaction_id.wrapping_add(1), Ordering::Release);
		Transaction::new(transaction_id, self)
	}
}

#[cfg(test)]
mod tests {
	use mockall::{predicate::*, Sequence};

	use crate::files::segment::PAGE_BODY_SIZE;

	use self::{
		cache::MockPageCacheApi,
		physical::MockPhysicalStorageApi,
		test_helpers::{page_id, wal_index},
		wal::MockWalApi,
	};

	use super::*;

	#[test]
	fn recover() {
		// expect
		let mut physical = MockPhysicalStorageApi::new();
		let mut cache = MockPageCacheApi::new();
		let mut wal = MockWalApi::new();
		wal.expect_recover().returning(|handler| {
			handler(wal::PartialWriteOp {
				index: wal_index!(69, 420),
				page_id: page_id!(1, 2),
				offset: 10,
				buf: &[1, 2, 3],
			})
			.unwrap();
			handler(wal::PartialWriteOp {
				index: wal_index!(10, 24),
				page_id: page_id!(4, 5),
				offset: 12,
				buf: &[2, 2, 1],
			})
			.unwrap();
			Ok(())
		});
		let mut seq = Sequence::new();

		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(1, 2)))
			.returning(|_| {
				let mut guard = MockPageWriteGuardApi::new();
				guard
					.expect_body_mut()
					.returning(|| vec![0; PAGE_BODY_SIZE]);
				guard.expect_body().return_const(vec![10; PAGE_BODY_SIZE]);
				guard
					.expect_write()
					.with(eq(10), eq([1, 2, 3]), eq(wal_index!(69, 420)));
				guard
			});
		physical
			.expect_read()
			.once()
			.in_sequence(&mut seq)
			.withf(|read_op| read_op.page_id == page_id!(1, 2))
			.returning(|read_op| {
				read_op.buf.fill(0);
				Ok(wal_index!(69, 420))
			});
		physical
			.expect_write()
			.once()
			.in_sequence(&mut seq)
			.withf(|write_op| {
				write_op.wal_index == wal_index!(69, 420)
					&& write_op.page_id == page_id!(1, 2)
					&& write_op.buf == [10; PAGE_BODY_SIZE]
			})
			.returning(|_| Ok(()));
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(4, 5)))
			.returning(|_| {
				let mut guard = MockPageWriteGuardApi::new();
				guard
					.expect_body_mut()
					.returning(|| vec![0; PAGE_BODY_SIZE]);
				guard.expect_body().return_const(vec![20; PAGE_BODY_SIZE]);
				guard
					.expect_write()
					.with(eq(12), eq([2, 2, 1]), eq(wal_index!(10, 24)));
				guard
			});
		physical
			.expect_read()
			.once()
			.in_sequence(&mut seq)
			.withf(|read_op| read_op.page_id == page_id!(4, 5))
			.returning(|read_op| {
				read_op.buf.fill(0);
				Ok(wal_index!(10, 24))
			});
		physical
			.expect_write()
			.once()
			.in_sequence(&mut seq)
			.withf(|write_op| {
				write_op.wal_index == wal_index!(10, 24)
					&& write_op.page_id == page_id!(4, 5)
					&& write_op.buf == [20; PAGE_BODY_SIZE]
			})
			.returning(|_| Ok(()));
		// given
		let page_storage = PageStorage::new(physical, cache, wal);

		// when
		page_storage.recover().unwrap();
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_id;
	pub(super) use crate::files::test_helpers::wal_index;
}
