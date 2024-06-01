use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::executor::ThreadPool;
use log::warn;
use thiserror::Error;

#[cfg(test)]
use mockall::{automock, mock};

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

	#[error("The maximum number of in-flight transactions has been reached")]
	TransactionLimitReached,

	#[error(transparent)]
	File(#[from] FileError),
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct PageStorageConfig {
	pub physical_storage: PhysicalStorageConfig,
	pub page_cache: PageCacheConfig,
	pub wal: WalConfig,
}

#[cfg_attr(test, automock)]
pub(crate) trait ReadPage {
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
}

impl<T: ReadPage> ReadPage for &T {
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		(**self).read(page_id, offset, buf)
	}
}

impl<T: ReadPage> ReadPage for &mut T {
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		(**self).read(page_id, offset, buf)
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait WritePage {
	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError>;
}

impl<T: WritePage> WritePage for &mut T {
	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		(*self).write(page_id, offset, buf)
	}
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
	completed: bool,
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
			completed: false,
		}
	}

	fn acquire_lock(&mut self, page_id: PageId) -> Result<(), StorageError> {
		if let Entry::Vacant(e) = self.locks.entry(page_id) {
			let guard = self.storage.write_guard(page_id)?;
			e.insert(guard);
		}
		Ok(())
	}

	fn undo_impl(&mut self) -> Result<(), StorageError> {
		self.storage.wal.undo(self.id, |write_op| {
			let Some(guard) = self.locks.get_mut(&write_op.page_id) else {
				panic!("An undo operation tried to undo a write to a page that the transaction did not access!");
			};
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
			Ok(())
		})?;
		self.storage.transaction_enumerator.end();
		Ok(())
	}
}

impl<'a, PS, PC, W> Drop for Transaction<'a, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn drop(&mut self) {
		if !self.completed {
			warn!("A transaction was dropped without being completed!");
			self.undo_impl()
				.expect("A transaction was dropped without being completed, and failed to undo!");
		}
	}
}

pub(crate) trait TransactionApi: ReadPage + WritePage {
	fn id(&self) -> u64;
	fn commit(self) -> Result<(), StorageError>;
	fn undo(self) -> Result<(), StorageError>;
}

impl<'a, PS, PC, W> ReadPage for Transaction<'a, PS, PC, W>
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
}

impl<'a, PS, PC, W> WritePage for Transaction<'a, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		self.acquire_lock(page_id)?;
		let guard = self.locks.get_mut(&page_id).unwrap();
		let mut from: Box<[u8]> = vec![0; buf.len()].into();
		guard.read(offset, &mut from);

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
}

impl<'a, PS, PC, W> TransactionApi for Transaction<'a, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn id(&self) -> u64 {
		self.id
	}

	fn commit(mut self) -> Result<(), StorageError> {
		self.storage.wal.log_commit(wal::CommitLog {
			transaction_id: self.id,
		})?;
		self.storage.transaction_enumerator.end();
		self.completed = true;
		Ok(())
	}

	fn undo(mut self) -> Result<(), StorageError> {
		self.undo_impl()?;
		self.completed = true;
		Ok(())
	}
}

#[cfg(test)]
mock! {
	pub(crate) TransactionApi {}

	impl ReadPage for TransactionApi {
		fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	}
	impl WritePage for TransactionApi {
		fn write(&mut self, page_id: PageId, offset: usize, buf: &[u8]) -> Result<(), StorageError>;
	}
	impl TransactionApi for TransactionApi {
		fn id(&self) -> u64;
		fn commit(self) -> Result<(), StorageError>;
		fn undo(self) -> Result<(), StorageError>;
	}
}

#[derive(Debug)]
struct TransactionEnumerator {
	next_id: AtomicU64,
	num_transactions: AtomicU64,
}

impl TransactionEnumerator {
	fn new() -> Self {
		Self {
			next_id: AtomicU64::new(0),
			num_transactions: AtomicU64::new(0),
		}
	}

	fn begin(&self) -> Option<u64> {
		let num_transactions = self.num_transactions.load(Ordering::Acquire);
		let num_transactions = num_transactions.checked_add(1)?;
		self.num_transactions
			.store(num_transactions, Ordering::Release);
		let id = self.next_id.load(Ordering::Acquire);
		self.next_id.store(id.wrapping_add(1), Ordering::Release);
		Some(id)
	}

	fn end(&self) {
		let num_transactions = self.num_transactions.load(Ordering::Acquire);
		self.num_transactions
			.store(num_transactions.saturating_sub(1), Ordering::Release);
	}
}

pub(crate) struct PageStorage<PS = PhysicalStorage, PC = PageCache, W = Wal>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	physical: Arc<PS>,
	cache: PC,
	wal: W,
	transaction_enumerator: TransactionEnumerator,
}

impl PageStorage {
	pub fn create(
		folder: Arc<DatabaseFolder>,
		thread_pool: Arc<ThreadPool>,
		config: &PageStorageConfig,
	) -> Result<Self, StorageError> {
		let physical_storage = Arc::new(PhysicalStorage::new(
			Arc::clone(&folder),
			&config.physical_storage,
		));
		Ok(Self::new(
			Arc::clone(&physical_storage),
			PageCache::new(
				&config.page_cache,
				Arc::clone(&physical_storage),
				Arc::clone(&thread_pool),
			),
			Wal::create(Arc::clone(&folder), thread_pool, &config.wal)?,
		))
	}

	pub fn open(
		folder: Arc<DatabaseFolder>,
		thread_pool: Arc<ThreadPool>,
		config: &PageStorageConfig,
	) -> Result<Self, StorageError> {
		let physical_storage = Arc::new(PhysicalStorage::new(
			Arc::clone(&folder),
			&config.physical_storage,
		));
		Ok(Self::new(
			Arc::clone(&physical_storage),
			PageCache::new(
				&config.page_cache,
				Arc::clone(&physical_storage),
				Arc::clone(&thread_pool),
			),
			Wal::open(Arc::clone(&folder), thread_pool, &config.wal)?,
		))
	}
}

impl<PS, PC, W> PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn new(physical: Arc<PS>, cache: PC, wal: W) -> Self {
		Self {
			physical,
			cache,
			wal,
			transaction_enumerator: TransactionEnumerator::new(),
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
		if let Some(guard) = self.cache.load(page_id) {
			return Ok(guard);
		}
		let guard = self.load_into_cache(page_id)?;
		Ok(self.cache.downgrade_guard(guard))
	}

	fn write_guard(&self, page_id: PageId) -> Result<PC::WriteGuard<'_>, StorageError> {
		if let Some(guard) = self.cache.load_mut(page_id) {
			return Ok(guard);
		}
		self.load_into_cache(page_id)
	}
}

pub(crate) trait PageStorageApi: ReadPage {
	type Transaction<'a>: TransactionApi + 'a
	where
		Self: 'a;

	fn recover(&self) -> Result<(), StorageError>;
	fn transaction<'a>(&'a self) -> Result<Self::Transaction<'a>, StorageError>;
}
impl<PS, PC, W> ReadPage for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		self.read_guard(page_id)?.read(offset, buf);
		Ok(())
	}
}

impl<PS, PC, W> PageStorageApi for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	type Transaction<'a> = Transaction<'a, PS, PC, W> where Self: 'a;

	fn recover(&self) -> Result<(), StorageError> {
		self.wal.recover(&mut |write_op| {
			let mut guard = self.write_guard(write_op.page_id)?;
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
			self.physical.write(WriteOp {
				wal_index: write_op.index,
				page_id: write_op.page_id,
				buf: guard.body(),
			})?;
			Ok(())
		})
	}

	fn transaction(&self) -> Result<Transaction<'_, PS, PC, W>, StorageError> {
		let Some(transaction_id) = self.transaction_enumerator.begin() else {
			return Err(StorageError::TransactionLimitReached);
		};
		Ok(Transaction::new(transaction_id, self))
	}
}

#[cfg(test)]
mock! {
	pub(crate) PageStorageApi {}

	impl ReadPage for PageStorageApi {
		fn read(&self, page_id: PageId, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	}

	impl PageStorageApi for PageStorageApi {
		type Transaction<'a> = MockTransactionApi;

		fn recover(&self) -> Result<(), StorageError>;
		fn transaction<'a>(&'a self) -> Result<MockTransactionApi, StorageError>;
	}
}

#[cfg(test)]
mod tests {
	use mockall::{predicate::*, Sequence};
	use pretty_assertions::assert_buf_eq;
	use tempfile::tempdir;
	use test::Bencher;
	use tests::wal::{CommitLog, WriteLog};

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
			.expect_load_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(1, 2)))
			.returning(|_| None);
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
				Ok(Some(wal_index!(69, 420)))
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
			.expect_load_mut()
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
				Some(guard)
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
		let page_storage = PageStorage::new(Arc::new(physical), cache, wal);

		// when
		page_storage.recover().unwrap();
	}

	#[test]
	fn read() {
		// expect
		let mut physical = MockPhysicalStorageApi::new();
		let mut cache = MockPageCacheApi::new();
		let wal = MockWalApi::new();
		let mut seq = Sequence::new();
		cache
			.expect_load()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(69, 420)))
			.returning(|_| None);
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(69, 420)))
			.returning(|_| {
				let mut guard = MockPageWriteGuardApi::new();
				guard
					.expect_body_mut()
					.returning(|| vec![0; PAGE_BODY_SIZE]);
				guard
			});
		physical
			.expect_read()
			.once()
			.in_sequence(&mut seq)
			.withf(|read_op| read_op.page_id == page_id!(69, 420))
			.returning(|read_op| {
				read_op
					.buf
					.iter_mut()
					.enumerate()
					.for_each(|(i, b)| *b = i as u8);
				Ok(Some(wal_index!(1, 2)))
			});
		cache
			.expect_downgrade_guard()
			.once()
			.in_sequence(&mut seq)
			.returning(|_| {
				let mut guard = MockPageReadGuardApi::new();
				guard
					.expect_read()
					.with(eq(10), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[10, 11, 12, 13, 14]);
					});
				guard
			});

		// given
		let storage = PageStorage::new(Arc::new(physical), cache, wal);

		// when
		let mut buf = [0; 5];
		storage.read(page_id!(69, 420), 10, &mut buf).unwrap();

		// then
		assert_buf_eq!(buf, [10, 11, 12, 13, 14]);
	}

	#[test]
	fn transaction() {
		// expect
		let mut physical = MockPhysicalStorageApi::new();
		let mut cache = MockPageCacheApi::new();
		let mut wal = MockWalApi::new();

		let mut seq = Sequence::new();
		cache
			.expect_load_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(1, 2)))
			.returning(|_| None);
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(1, 2)))
			.returning(|_| {
				let mut guard = MockPageWriteGuardApi::new();
				let mut seq = Sequence::new();
				guard
					.expect_body_mut()
					.once()
					.in_sequence(&mut seq)
					.returning(|| vec![0; PAGE_BODY_SIZE]);
				guard
					.expect_read()
					.once()
					.in_sequence(&mut seq)
					.with(eq(10), always())
					.returning(|_, buf| buf.copy_from_slice(&[69, 25]));
				guard.expect_write().once().in_sequence(&mut seq).with(
					eq(10),
					eq([1, 2]),
					eq(wal_index!(24, 25)),
				);
				guard
					.expect_read()
					.once()
					.in_sequence(&mut seq)
					.with(eq(10), always())
					.returning(|_, buf| buf.copy_from_slice(&[1, 2]));
				guard
			});
		physical
			.expect_read()
			.once()
			.in_sequence(&mut seq)
			.withf(|read_op| read_op.page_id == page_id!(1, 2))
			.returning(|read_op| {
				read_op.buf.fill(0);
				Ok(Some(wal_index!(69, 420)))
			});
		wal.expect_log_write()
			.once()
			.in_sequence(&mut seq)
			.withf(|write_log| {
				*write_log
					== WriteLog {
						transaction_id: 0,
						page_id: page_id!(1, 2),
						offset: 10,
						from: &[69, 25],
						to: &[1, 2],
					}
			})
			.returning(|_| Ok(wal_index!(24, 25)));
		wal.expect_log_commit()
			.once()
			.in_sequence(&mut seq)
			.with(eq(CommitLog { transaction_id: 0 }))
			.returning(|_| Ok(wal_index!(24, 25)));

		// given
		let storage = PageStorage::new(Arc::new(physical), cache, wal);

		// when
		let mut t = storage.transaction().unwrap();
		t.write(page_id!(1, 2), 10, &[1, 2]).unwrap();
		let mut received = [0; 2];
		t.read(page_id!(1, 2), 10, &mut received).unwrap();
		t.commit().unwrap();

		// then
		assert_buf_eq!(received, [1, 2]);
	}

	#[test]
	fn integration_transaction() {
		let tempdir = tempdir().unwrap();

		let folder = Arc::new(DatabaseFolder::open(tempdir.path().to_path_buf()));
		let thread_pool = Arc::new(ThreadPool::new().unwrap());
		let page_storage = PageStorage::create(folder, thread_pool, &Default::default()).unwrap();

		let mut t = page_storage.transaction().unwrap();

		t.write(page_id!(69, 420), 25, &[1, 2, 3, 4]).unwrap();
		let mut data = [0; 4];
		t.read(page_id!(69, 420), 25, &mut data).unwrap();
		t.commit().unwrap();

		assert_buf_eq!(data, [1, 2, 3, 4]);
	}

	#[bench]
	fn bench_transaction(b: &mut Bencher) {
		let tempdir = tempdir().unwrap();

		let folder = Arc::new(DatabaseFolder::open(tempdir.path().to_path_buf()));
		let thread_pool = Arc::new(ThreadPool::new().unwrap());
		let page_storage = PageStorage::create(folder, thread_pool, &Default::default()).unwrap();

		b.iter(|| {
			let mut t = page_storage.transaction().unwrap();

			t.write(page_id!(69, 420), 25, &[1, 2, 3, 4]).unwrap();
			t.read(page_id!(69, 420), 25, &mut [0; 4]).unwrap();
			t.commit().unwrap();
		})
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_id;
	pub(super) use crate::files::test_helpers::wal_index;
}
