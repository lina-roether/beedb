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
use crate::page_store::cache::{MockPageReadGuardApi, MockPageWriteGuardApi};

use crate::files::DatabaseFolder;
use crate::files::FileError;
use crate::page_store::cache::PageWriteGuardApi;

pub(crate) use crate::files::PageAddress;
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

pub(crate) trait ReadPage {
	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
}

impl<T: ReadPage> ReadPage for &T {
	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		(**self).read(offset, buf)
	}
}

impl<T: ReadPage> ReadPage for &mut T {
	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		(**self).read(offset, buf)
	}
}

pub(crate) trait WritePage {
	fn write(&mut self, offset: usize, buf: &[u8]) -> Result<(), StorageError>;
}

impl<T: WritePage> WritePage for &mut T {
	fn write(&mut self, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		(*self).write(offset, buf)
	}
}

enum WriteablePageGuard<'t, 'a, PC>
where
	PC: PageCacheApi + 't,
{
	Shared(PC::ReadGuard<'t>),
	Exclusive(&'a PC::WriteGuard<'t>),
}

pub(crate) struct Page<'t, 'a, PC>
where
	PC: PageCacheApi + 't,
{
	guard: WriteablePageGuard<'t, 'a, PC>,
}

impl<'t, 'a, PC> ReadPage for Page<'t, 'a, PC>
where
	PC: PageCacheApi + 't,
{
	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		match &self.guard {
			WriteablePageGuard::Shared(guard) => guard.read(offset, buf),
			WriteablePageGuard::Exclusive(guard) => guard.read(offset, buf),
		}
		Ok(())
	}
}

pub(crate) struct PageMut<'t, 'a, PC, W>
where
	PC: PageCacheApi + 't,
{
	page_address: PageAddress,
	transaction_id: u64,
	guard: &'a mut PC::WriteGuard<'t>,
	wal: &'a W,
}

impl<'t, 'a, PC, W> ReadPage for PageMut<'t, 'a, PC, W>
where
	PC: PageCacheApi + 'a,
{
	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError> {
		self.guard.read(offset, buf);
		Ok(())
	}
}

impl<'t, 'a, PC, W> WritePage for PageMut<'t, 'a, PC, W>
where
	PC: PageCacheApi + 'a,
	W: WalApi,
{
	fn write(&mut self, offset: usize, buf: &[u8]) -> Result<(), StorageError> {
		let mut from: Box<[u8]> = vec![0; buf.len()].into();
		self.guard.read(offset, &mut from);

		let wal_index = self.wal.log_write(wal::WriteLog {
			transaction_id: self.transaction_id,
			page_address: self.page_address,
			offset: u16::try_from(offset).expect("Write offset must be 16-bit!"),
			from: &from,
			to: buf,
		})?;
		self.guard.write(offset, buf, wal_index);
		Ok(())
	}
}

#[cfg(test)]
mock! {
	pub(crate) Page {}

	impl ReadPage for Page {
		fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	}
}

#[cfg(test)]
mock! {
	pub(crate) PageMut {}

	impl ReadPage for PageMut {
		fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), StorageError>;
	}

	impl WritePage for PageMut {
		fn write(&mut self, offset: usize, buf: &[u8]) -> Result<(), StorageError>;
	}
}

pub(crate) struct Transaction<'t, PS = PhysicalStorage, PC = PageCache, W = Wal>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	id: u64,
	locks: HashMap<PageAddress, PC::WriteGuard<'t>>,
	storage: &'t PageStorage<PS, PC, W>,
	completed: bool,
}

impl<'t, PS, PC, W> Transaction<'t, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	fn new(id: u64, storage: &'t PageStorage<PS, PC, W>) -> Self {
		Self {
			id,
			storage,
			locks: HashMap::new(),
			completed: false,
		}
	}

	fn acquire_lock(&mut self, page_address: PageAddress) -> Result<(), StorageError> {
		if let Entry::Vacant(e) = self.locks.entry(page_address) {
			let guard = self.storage.write_guard(page_address)?;
			e.insert(guard);
		}
		Ok(())
	}

	fn undo_impl(&mut self) -> Result<(), StorageError> {
		self.storage.wal.undo(self.id, |write_op| {
			let Some(guard) = self.locks.get_mut(&write_op.page_address) else {
				panic!("An undo operation tried to undo a write to a page that the transaction did not access!");
			};
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
			Ok(())
		})?;
		self.storage.transaction_enumerator.end();
		Ok(())
	}
}

impl<'t, PS, PC, W> Drop for Transaction<'t, PS, PC, W>
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

#[cfg_attr(test, automock(
    type Page = MockPage;
    type PageMut = MockPageMut;
))]
pub(crate) trait TransactionApi {
	type Page<'a>: ReadPage + 'a
	where
		Self: 'a;
	type PageMut<'a>: ReadPage + WritePage + 'a
	where
		Self: 'a;

	fn id(&self) -> u64;
	fn get_page(&self, page_address: PageAddress) -> Result<Self::Page<'_>, StorageError>;
	fn get_page_mut(
		&mut self,
		page_address: PageAddress,
	) -> Result<Self::PageMut<'_>, StorageError>;
	fn commit(self) -> Result<(), StorageError>;
	fn undo(self) -> Result<(), StorageError>;
}

impl<'t, PS, PC, W> TransactionApi for Transaction<'t, PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi + 't,
	W: WalApi + 't,
{
	type Page<'a> = Page<'t, 'a, PC> where Self: 'a;
	type PageMut<'a> = PageMut<'t, 'a, PC, W> where Self: 'a;

	fn id(&self) -> u64 {
		self.id
	}

	fn get_page(&self, page_address: PageAddress) -> Result<Self::Page<'_>, StorageError> {
		if let Some(guard) = self.locks.get(&page_address) {
			Ok(Page {
				guard: WriteablePageGuard::Exclusive(guard),
			})
		} else {
			Ok(Page {
				guard: WriteablePageGuard::Shared(self.storage.read_guard(page_address)?),
			})
		}
	}

	fn get_page_mut<'a>(
		&'a mut self,
		page_address: PageAddress,
	) -> Result<Self::PageMut<'a>, StorageError> {
		self.acquire_lock(page_address)?;
		let guard: &'a mut PC::WriteGuard<'t> = self.locks.get_mut(&page_address).unwrap();
		Ok(PageMut {
			page_address,
			transaction_id: self.id,
			guard,
			wal: &self.storage.wal,
		})
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

pub(crate) struct PageStorage<PS = PhysicalStorage, PC = PageCache, W = Wal> {
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
{
	fn new(physical: Arc<PS>, cache: PC, wal: W) -> Self {
		Self {
			physical,
			cache,
			wal,
			transaction_enumerator: TransactionEnumerator::new(),
		}
	}

	fn load_into_cache(
		&self,
		page_address: PageAddress,
	) -> Result<PC::WriteGuard<'_>, StorageError> {
		let mut guard = self.cache.store(page_address);
		if let Err(error) = self.physical.read(ReadOp {
			page_address,
			buf: guard.body_mut(),
		}) {
			self.cache.scrap(page_address);
			return Err(error);
		}
		Ok(guard)
	}

	fn read_guard(&self, page_address: PageAddress) -> Result<PC::ReadGuard<'_>, StorageError> {
		if let Some(guard) = self.cache.load(page_address) {
			return Ok(guard);
		}
		let guard = self.load_into_cache(page_address)?;
		Ok(self.cache.downgrade_guard(guard))
	}

	fn write_guard(&self, page_address: PageAddress) -> Result<PC::WriteGuard<'_>, StorageError> {
		if let Some(guard) = self.cache.load_mut(page_address) {
			return Ok(guard);
		}
		self.load_into_cache(page_address)
	}
}

#[cfg_attr(test, automock(
    type Page<'a> = MockPage;
    type Transaction<'a> = MockTransactionApi;
))]
pub(crate) trait PageStorageApi {
	type Page<'a>: ReadPage + 'a
	where
		Self: 'a;
	type Transaction<'a>: TransactionApi
	where
		Self: 'a;

	fn recover(&self) -> Result<(), StorageError>;
	fn get_page(&self, page_address: PageAddress) -> Result<Self::Page<'_>, StorageError>;
	fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError>;
	fn flush(&self);
	fn flush_sync(&self) -> Result<(), StorageError>;
}

impl<PS, PC, W> PageStorageApi for PageStorage<PS, PC, W>
where
	PS: PhysicalStorageApi,
	PC: PageCacheApi,
	W: WalApi,
{
	type Page<'a> = Page<'a, 'a, PC> where Self: 'a;
	type Transaction<'a> = Transaction<'a, PS, PC, W> where Self: 'a;

	fn recover(&self) -> Result<(), StorageError> {
		self.wal.recover(&mut |write_op| {
			let mut guard = self.write_guard(write_op.page_address)?;
			guard.write(write_op.offset.into(), write_op.buf, write_op.index);
			self.physical.write(WriteOp {
				wal_index: write_op.index,
				page_address: write_op.page_address,
				buf: guard.body(),
			})?;
			Ok(())
		})
	}

	fn get_page(&self, page_address: PageAddress) -> Result<Self::Page<'_>, StorageError> {
		Ok(Page {
			guard: WriteablePageGuard::Shared(self.read_guard(page_address)?),
		})
	}

	fn transaction(&self) -> Result<Transaction<'_, PS, PC, W>, StorageError> {
		let Some(transaction_id) = self.transaction_enumerator.begin() else {
			return Err(StorageError::TransactionLimitReached);
		};
		Ok(Transaction::new(transaction_id, self))
	}

	fn flush(&self) {
		self.cache.flush();
	}

	fn flush_sync(&self) -> Result<(), StorageError> {
		self.cache.flush_sync()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		fs::File,
		io::{Read, Seek, SeekFrom},
	};

	use mockall::{predicate::*, Sequence};
	use pretty_assertions::assert_buf_eq;
	use tempfile::tempdir;
	use test::Bencher;
	use tests::wal::{CommitLog, WriteLog};

	use crate::{consts::PAGE_SIZE, files::segment::PAGE_BODY_SIZE, utils::units::KIB};

	use self::{
		cache::MockPageCacheApi,
		physical::MockPhysicalStorageApi,
		test_helpers::{page_address, wal_index},
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
				page_address: page_address!(1, 2),
				offset: 10,
				buf: &[1, 2, 3],
			})
			.unwrap();
			handler(wal::PartialWriteOp {
				index: wal_index!(10, 24),
				page_address: page_address!(4, 5),
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
			.with(eq(page_address!(1, 2)))
			.returning(|_| None);
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_address!(1, 2)))
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
			.withf(|read_op| read_op.page_address == page_address!(1, 2))
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
					&& write_op.page_address == page_address!(1, 2)
					&& write_op.buf == [10; PAGE_BODY_SIZE]
			})
			.returning(|_| Ok(()));
		cache
			.expect_load_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_address!(4, 5)))
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
					&& write_op.page_address == page_address!(4, 5)
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
			.with(eq(page_address!(69, 420)))
			.returning(|_| None);
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_address!(69, 420)))
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
			.withf(|read_op| read_op.page_address == page_address!(69, 420))
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
		storage
			.get_page(page_address!(69, 420))
			.unwrap()
			.read(10, &mut buf)
			.unwrap();

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
			.with(eq(page_address!(1, 2)))
			.returning(|_| None);
		cache
			.expect_store()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_address!(1, 2)))
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
			.withf(|read_op| read_op.page_address == page_address!(1, 2))
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
						page_address: page_address!(1, 2),
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
		t.get_page_mut(page_address!(1, 2))
			.unwrap()
			.write(10, &[1, 2])
			.unwrap();
		let mut received = [0; 2];
		t.get_page(page_address!(1, 2))
			.unwrap()
			.read(10, &mut received)
			.unwrap();
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

		t.get_page_mut(page_address!(69, 420))
			.unwrap()
			.write(25, &[1, 2, 3, 4])
			.unwrap();
		let mut data = [0; 4];
		t.get_page(page_address!(69, 420))
			.unwrap()
			.read(25, &mut data)
			.unwrap();
		t.commit().unwrap();

		assert_buf_eq!(data, [1, 2, 3, 4]);

		page_storage.flush_sync().unwrap();

		let mut segment_file = File::open(tempdir.path().join("segments/69")).unwrap();
		const OFFSET: usize = 420 * PAGE_SIZE + 19;
		segment_file
			.seek(SeekFrom::Start(OFFSET.try_into().unwrap()))
			.unwrap();
		let mut buf = [0; PAGE_BODY_SIZE];
		segment_file.read_exact(&mut buf).unwrap();

		let mut expected = [0; PAGE_BODY_SIZE];
		expected[25..29].copy_from_slice(&[1, 2, 3, 4]);
		assert_buf_eq!(buf, expected);
	}

	#[bench]
	fn bench_write_and_commit(b: &mut Bencher) {
		let tempdir = tempdir().unwrap();

		let folder = Arc::new(DatabaseFolder::open(tempdir.path().to_path_buf()));
		let thread_pool = Arc::new(ThreadPool::new().unwrap());
		let page_storage = PageStorage::create(folder, thread_pool, &Default::default()).unwrap();

		const DATA: &[u8] = &[69; 16 * KIB];

		b.iter(|| {
			let mut t = page_storage.transaction().unwrap();

			let mut page = t.get_page_mut(page_address!(69, 420)).unwrap();
			page.write(25, DATA).unwrap();

			t.commit().unwrap();
		})
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	pub(crate) use crate::files::test_helpers::page_address;
	pub(super) use crate::files::test_helpers::wal_index;
}
