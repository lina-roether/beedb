use std::{
	fs::File,
	sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use static_assertions::assert_impl_all;

use crate::{index::PageId, wal::Wal};

use super::err::Error;

pub struct TransactionManager {
	sequence_counter: AtomicU64,
	transaction_counter: AtomicU64,
	wal: Mutex<Wal<File>>,
}

assert_impl_all!(TransactionManager: Send, Sync);

impl TransactionManager {
	pub fn new(wal: Wal<File>) -> Self {
		Self {
			sequence_counter: AtomicU64::new(0),
			transaction_counter: AtomicU64::new(0),
			wal: Mutex::new(wal),
		}
	}

	pub fn begin(&self) -> u64 {
		self.next_tid()
	}

	pub fn track_write(&self, tid: u64, page_id: PageId, data: &[u8]) {
		let mut wal = self.wal.lock();
		wal.log_write(tid, self.next_seq(), page_id, data);
	}

	pub fn commit(&self, tid: u64) -> Result<(), Error> {
		let mut wal = self.wal.lock();
		wal.commit(tid).map_err(Error::WalWrite)?;
		Ok(())
	}

	#[inline]
	fn next_seq(&self) -> u64 {
		self.sequence_counter.fetch_add(1, Ordering::SeqCst)
	}

	#[inline]
	fn next_tid(&self) -> u64 {
		self.transaction_counter.fetch_add(1, Ordering::SeqCst)
	}
}

#[derive(Debug)]
struct TransactionTableRow {
	last_seq: u64,
	tid: u64,
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use crate::wal;

	use super::*;

	#[test]
	// There seems to be some sort of bug in the standard library that breaks this test under miri
	// :/
	#[cfg_attr(miri, ignore)]
	fn simple_transaction() {
		let dir = tempdir().unwrap();
		Wal::init_file(
			dir.path().join("writes.acnl"),
			wal::InitParams { page_size: 8 },
		)
		.unwrap();

		let tm = TransactionManager::new(
			Wal::load_file(
				dir.path().join("writes.acnl"),
				wal::LoadParams { page_size: 8 },
			)
			.unwrap(),
		);
		let tid = tm.begin();
		tm.track_write(tid, PageId::new(0, 1), &[1, 2, 3, 4, 5, 6, 7]);
		tm.commit(tid).unwrap();
	}
}
