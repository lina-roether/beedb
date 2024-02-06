use std::{
	collections::{hash_map::Entry, HashMap},
	fs::File,
	num::NonZeroU64,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};

use parking_lot::Mutex;
use static_assertions::assert_impl_all;

use crate::{
	cache::{PageCache, PageWriteGuard},
	disk::{
		storage::{self, Storage, StorageApi},
		wal::{self, Wal, WalApi},
	},
	id::PageId,
	pages::{ReadOp, WriteOp},
};

use super::{err::Error, recovery::RecoveryManager};

pub(super) struct TransactionManager<Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	tid_counter: AtomicU64,
	cache: Arc<PageCache<Storage>>,
	state: Arc<Mutex<State<Storage, Wal>>>,
}

assert_impl_all!(TransactionManager<Storage, Wal<File>>: Send, Sync);

impl<Storage, Wal> TransactionManager<Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	pub fn new(cache: Arc<PageCache<Storage>>, recovery: RecoveryManager<Storage, Wal>) -> Self {
		Self {
			tid_counter: AtomicU64::new(0),
			cache,
			state: Arc::new(Mutex::new(State::new(recovery))),
		}
	}

	pub fn begin(&self) -> Transaction<Storage, Wal> {
		Transaction::new(self.next_tid(), &self.state, &self.cache)
	}

	#[inline]
	fn next_tid(&self) -> u64 {
		self.tid_counter.fetch_add(1, Ordering::SeqCst)
	}
}

struct State<Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	recovery: RecoveryManager<Storage, Wal>,
	seq_counter: u64,
}

impl<Storage, Wal> State<Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	fn new(recovery: RecoveryManager<Storage, Wal>) -> Self {
		Self {
			recovery,
			seq_counter: 0,
		}
	}

	#[inline]
	fn next_seq(&mut self) -> NonZeroU64 {
		self.seq_counter += 1;
		NonZeroU64::new(self.seq_counter).unwrap()
	}
}

pub(crate) struct Transaction<'a, Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	tid: u64,
	last_seq: Option<NonZeroU64>,
	state: &'a Mutex<State<Storage, Wal>>,
	cache: &'a PageCache<Storage>,
	locks: HashMap<PageId, PageWriteGuard<'a>>,
}

impl<'a, Storage, Wal> Transaction<'a, Storage, Wal>
where
	Storage: StorageApi,
	Wal: WalApi,
{
	fn new(tid: u64, state: &'a Mutex<State<Storage, Wal>>, cache: &'a PageCache<Storage>) -> Self {
		Self {
			tid,
			last_seq: None,
			state,
			cache,
			locks: HashMap::new(),
		}
	}

	pub fn read(&mut self, page_id: PageId, op: ReadOp) -> Result<(), storage::Error> {
		debug_assert!(op.range().end <= self.cache.page_size().into());

		if let Some(lock) = self.locks.get(&page_id) {
			op.bytes.copy_from_slice(&lock[op.range()]);
		} else {
			let page = self.cache.read_page(page_id)?;
			op.bytes.copy_from_slice(&page[op.range()]);
		}

		Ok(())
	}

	pub fn write(&mut self, page_id: PageId, op: WriteOp) -> Result<(), Error> {
		debug_assert!(op.range().end <= self.cache.page_size().into());

		let mut before: Box<[u8]> = vec![0; op.bytes.len()].into();
		self.read(page_id, ReadOp::new(op.start, &mut before))?;

		self.track_write(page_id, &before, op)?;

		if let Entry::Vacant(e) = self.locks.entry(page_id) {
			e.insert(self.cache.write_page(page_id)?);
		}
		let lock = self.locks.get_mut(&page_id).unwrap();
		lock[op.range()].copy_from_slice(op.bytes);
		Ok(())
	}

	pub fn cancel(mut self) -> Result<(), Error> {
		let mut state = self.state.lock();
		let (seq, prev_seq) = self.next_seq(&mut state);
		state.recovery.cancel_transaction(wal::ItemInfo {
			tid: self.tid,
			seq,
			prev_seq,
		})
	}

	pub fn commit(mut self) -> Result<(), Error> {
		let mut state = self.state.lock();
		let (seq, prev_seq) = self.next_seq(&mut state);
		state.recovery.commit_transaction(wal::ItemInfo {
			tid: self.tid,
			seq,
			prev_seq,
		})
	}

	fn create_rollback_write(
		&self,
		page_id: PageId,
	) -> Result<(PageId, Box<[u8]>), storage::Error> {
		let page = self.cache.read_page(page_id)?;
		Ok((page_id, page.as_ref().into()))
	}

	fn apply_write(&self, page_id: PageId, data: &[u8]) -> Result<(), storage::Error> {
		let mut page = self.cache.write_page(page_id)?;
		debug_assert!(data.len() <= page.len());

		page[0..data.len()].copy_from_slice(data);
		Ok(())
	}

	fn track_write(&mut self, page_id: PageId, before: &[u8], op: WriteOp) -> Result<(), Error> {
		let mut state = self.state.lock();

		let (seq, prev_seq) = self.next_seq(&mut state);
		state
			.recovery
			.track_write(
				wal::ItemInfo {
					tid: self.tid,
					seq,
					prev_seq,
				},
				wal::WriteInfo {
					page_id,
					start: op.start as u16,
					before,
					after: op.bytes,
				},
			)
			.unwrap();
		Ok(())
	}

	fn next_seq(&mut self, state: &mut State<Storage, Wal>) -> (NonZeroU64, Option<NonZeroU64>) {
		let seq = state.next_seq();
		let prev_seq = self.last_seq;
		self.last_seq = Some(seq);
		(seq, prev_seq)
	}
}

#[cfg(test)]
mod tests {

	use std::mem;

	use tempfile::tempdir;

	use crate::{
		consts::PAGE_SIZE_RANGE, disk::storage::Storage, manage::transaction::tests::wal::Wal,
	};

	use super::*;

	#[test]
	// There seems to be some sort of bug in the standard library that breaks this test under miri
	// :/
	#[cfg_attr(miri, ignore)]
	fn simple_transaction() {
		const PAGE_SIZE: u16 = *PAGE_SIZE_RANGE.start();

		let dir = tempdir().unwrap();
		Storage::init(
			dir.path(),
			storage::InitParams {
				page_size: PAGE_SIZE,
			},
		)
		.unwrap();
		Wal::init_file(dir.path().join("writes.acnl")).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));
		let recovery = RecoveryManager::new(Arc::clone(&cache), wal);

		cache.write_page(PageId::new(0, 1)).unwrap().fill(0);
		cache.write_page(PageId::new(0, 2)).unwrap().fill(0);

		let tm = TransactionManager::new(cache, recovery);
		let mut t = tm.begin();
		let mut buf = vec![0; PAGE_SIZE as usize];

		t.write(
			PageId::new(0, 1),
			WriteOp::new(0, &[25; PAGE_SIZE as usize]),
		)
		.unwrap();
		t.read(PageId::new(0, 1), ReadOp::new(0, &mut buf)).unwrap();
		assert!(buf.iter().all(|b| *b == 25));

		t.write(
			PageId::new(0, 1),
			WriteOp::new(10, &[69; PAGE_SIZE as usize - 10]),
		)
		.unwrap();
		t.read(PageId::new(0, 1), ReadOp::new(0, &mut buf)).unwrap();

		assert!(buf[0..10].iter().all(|b| *b == 25));
		assert!(buf[10..].iter().all(|b| *b == 69));

		t.commit().unwrap();

		mem::drop(tm);

		let mut wal = Wal::load_file(dir.path().join("writes.acnl")).unwrap();
		let wal_items: Vec<wal::Item> = wal.iter().unwrap().map(|i| i.unwrap()).collect();
		assert_eq!(
			wal_items,
			vec![
				wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(1).unwrap(),
						prev_seq: None
					},
					data: wal::ItemData::Write {
						page_id: PageId::new(0, 1),
						start: 0,
						before: [0; PAGE_SIZE as usize].into(),
						after: [25; PAGE_SIZE as usize].into()
					}
				},
				wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(2).unwrap(),
						prev_seq: NonZeroU64::new(1)
					},
					data: wal::ItemData::Write {
						page_id: PageId::new(0, 1),
						start: 10,
						before: [25; PAGE_SIZE as usize - 10].into(),
						after: [69; PAGE_SIZE as usize - 10].into()
					}
				},
				wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(3).unwrap(),
						prev_seq: NonZeroU64::new(2)
					},
					data: wal::ItemData::Commit
				},
			]
		)
	}
}
