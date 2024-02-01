use std::{
	collections::{hash_map::Entry, HashMap},
	fmt::Display,
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
		storage,
		wal::{self, Wal},
	},
	id::PageId,
	utils::aligned_buf::AlignedBuffer,
};

use super::err::Error;

pub(super) struct TransactionManager {
	tid_counter: AtomicU64,
	cache: Arc<PageCache>,
	state: Arc<Mutex<State>>,
}

assert_impl_all!(TransactionManager: Send, Sync);

impl TransactionManager {
	pub fn new(cache: Arc<PageCache>, wal: Wal<File>) -> Self {
		let tm = Self {
			tid_counter: AtomicU64::new(0),
			cache,
			state: Arc::new(Mutex::new(State::new(wal))),
		};
		tm.recover_from_wal();
		tm
	}

	pub fn begin(&self) -> Transaction {
		Transaction {
			tid: self.next_tid(),
			cache: &self.cache,
			state: &self.state,
			locks: HashMap::new(),
		}
	}

	#[inline]
	fn next_tid(&self) -> u64 {
		self.tid_counter.fetch_add(1, Ordering::SeqCst)
	}

	fn recover_from_wal(&self) {
		let mut state = self.state.lock();

		#[allow(clippy::type_complexity)]
		let mut transactions: HashMap<u64, Vec<(PageId, Box<[u8]>)>> = HashMap::new();

		let items_iter = state
			.wal
			.iter()
			.unwrap_or_else(|err| Self::panic_recovery_failed(err));

		for item in items_iter {
			let item = item.unwrap_or_else(|err| Self::panic_recovery_failed(err));
			match item {
				wal::Item::Write { tid, page_id, diff } => {
					let buffered_writes = transactions.entry(tid).or_default();
					buffered_writes.push((page_id, diff));
				}
				wal::Item::Commit(tid) => {
					let Some(buffered_writes) = transactions.get(&tid) else {
						continue;
					};
					for (page_id, diff) in buffered_writes {
						let mut page = self
							.cache
							.write_page(*page_id)
							.unwrap_or_else(|err| Self::panic_recovery_failed(err));

						for (byte, diff) in page.iter_mut().zip(diff.iter()) {
							*byte ^= *diff;
						}
					}
				}
				wal::Item::Cancel(tid) => {
					transactions.remove(&tid);
				}
			}
		}
	}

	fn panic_recovery_failed(err: impl Display) -> ! {
		panic!("Failed to recover from WAL: {err}\nStarting without recovering could leave the database in an inconsistent state.")
	}
}

struct State {
	wal: Wal<File>,
	seq_counter: u64,
}

impl State {
	fn new(wal: Wal<File>) -> Self {
		Self {
			wal,
			seq_counter: 0,
		}
	}

	#[inline]
	fn next_seq(&mut self) -> NonZeroU64 {
		self.seq_counter += 1;
		NonZeroU64::new(self.seq_counter).unwrap()
	}
}

pub(crate) struct Transaction<'a> {
	tid: u64,
	state: &'a Mutex<State>,
	cache: &'a PageCache,
	locks: HashMap<PageId, PageWriteGuard<'a>>,
}

impl<'a> Transaction<'a> {
	pub fn read(&mut self, page_id: PageId, buf: &mut [u8]) -> Result<(), storage::Error> {
		if let Some(lock) = self.locks.get(&page_id) {
			buf.copy_from_slice(lock);
		} else {
			let page = self.cache.read_page(page_id)?;
			buf.copy_from_slice(&page);
		}

		Ok(())
	}

	pub fn write(&mut self, page_id: PageId, data: &[u8]) -> Result<(), Error> {
		self.track_write(page_id, data)?;
		if let Entry::Vacant(e) = self.locks.entry(page_id) {
			e.insert(self.cache.write_page(page_id)?);
		}
		let lock = self.locks.get_mut(&page_id).unwrap();
		lock[0..data.len()].copy_from_slice(data);
		Ok(())
	}

	pub fn cancel(self) {
		self.track_cancel();
		todo!("This needs to rollback the changes written to the PageCache");
	}

	pub fn commit(self) -> Result<(), Error> {
		self.track_commit()?;
		Ok(())
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

	fn track_write(&mut self, page_id: PageId, data: &[u8]) -> Result<(), Error> {
		let mut state = self.state.lock();

		let mut diff: AlignedBuffer =
			AlignedBuffer::with_capacity(1, self.cache.page_size().into());
		self.read(page_id, &mut diff)?;

		for (diff, change) in diff.iter_mut().zip(data.iter()) {
			*diff ^= *change
		}

		let seq = state.next_seq();
		state.wal.push_write(self.tid, seq, page_id, &diff);
		Ok(())
	}

	fn track_cancel(&self) {
		let mut state = self.state.lock();
		let seq = state.next_seq();
		state.wal.push_cancel(self.tid, seq);
	}

	fn track_commit(&self) -> Result<(), Error> {
		let mut state = self.state.lock();
		let seq = state.next_seq();
		state.wal.push_commit(self.tid, seq);
		state.wal.flush().map_err(Error::WalWrite)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use std::mem;

	use tempfile::tempdir;

	use crate::{consts::DEFAULT_PAGE_SIZE, disk::storage::Storage};

	use super::*;

	#[test]
	// There seems to be some sort of bug in the standard library that breaks this test under miri
	// :/
	#[cfg_attr(miri, ignore)]
	fn simple_transaction() {
		let dir = tempdir().unwrap();
		Storage::init(dir.path(), storage::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		let wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = Arc::new(PageCache::new(storage, 100));

		cache.write_page(PageId::new(0, 1)).unwrap().fill(0);
		cache.write_page(PageId::new(0, 2)).unwrap().fill(0);

		let tm = TransactionManager::new(cache, wal);
		let mut t = tm.begin();
		let mut buf = vec![0; DEFAULT_PAGE_SIZE as usize];

		t.write(PageId::new(0, 1), &[25; DEFAULT_PAGE_SIZE as usize])
			.unwrap();
		t.read(PageId::new(0, 1), &mut buf).unwrap();
		assert!(buf.iter().all(|b| *b == 25));

		t.write(PageId::new(0, 2), &[69; DEFAULT_PAGE_SIZE as usize])
			.unwrap();
		t.read(PageId::new(0, 2), &mut buf).unwrap();
		assert!(buf.iter().all(|b| *b == 69));

		t.commit().unwrap();

		mem::drop(tm);

		let mut wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let wal_items: Vec<wal::Item> = wal.iter().unwrap().map(|i| i.unwrap()).collect();
		assert_eq!(
			wal_items,
			vec![
				wal::Item::Write {
					tid: 0,
					page_id: PageId::new(0, 1),
					diff: [25; DEFAULT_PAGE_SIZE as usize].into(),
				},
				wal::Item::Write {
					tid: 0,
					page_id: PageId::new(0, 2),
					diff: [69; DEFAULT_PAGE_SIZE as usize].into(),
				},
				wal::Item::Commit(0)
			]
		)
	}
}
