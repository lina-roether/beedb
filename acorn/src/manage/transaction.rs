use std::{
	collections::HashMap,
	sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use static_assertions::assert_impl_all;

use crate::index::PageId;

use super::err::Error;

#[repr(u8)]
pub enum Operation {
	Begin,
	Write(PageId),
	Commit,
}

/*
 * TODO: This thing doesn't do much at the moment, but it should handle WAL
 * stuff
 */

pub struct TransactionManager {
	sequence_counter: AtomicU64,
	transaction_counter: AtomicU64,
	transaction_table: Mutex<HashMap<u64, TransactionTableRow>>,
}

assert_impl_all!(TransactionManager: Send, Sync);

impl TransactionManager {
	pub fn new() -> Self {
		Self {
			sequence_counter: AtomicU64::new(0),
			transaction_counter: AtomicU64::new(0),
			transaction_table: Mutex::new(HashMap::new()),
		}
	}

	fn operation_raw(
		&self,
		transaction_table: &mut HashMap<u64, TransactionTableRow>,
		tid: u64,
		_operation: Operation,
		_before: &[u8],
		_after: &[u8],
	) -> Result<(), Error> {
		let seq = self.next_seq();
		let Some(row) = transaction_table.get_mut(&tid) else {
			return Err(Error::NoSuchTransaction(tid));
		};
		row.last_seq = seq;

		// Doesn't do anything at the moment
		Ok(())
	}

	pub fn begin(&self) -> Result<u64, Error> {
		let mut transaction_table = self.transaction_table.lock();
		let tid = self.next_tid();
		let begin_seq = self.next_seq();
		transaction_table.insert(
			tid,
			TransactionTableRow {
				last_seq: begin_seq,
				tid,
			},
		);
		self.operation_raw(&mut transaction_table, tid, Operation::Begin, &[], &[])?;
		Ok(tid)
	}

	pub fn operation(
		&self,
		tid: u64,
		operation: Operation,
		before: &[u8],
		after: &[u8],
	) -> Result<(), Error> {
		let mut transaction_table = self.transaction_table.lock();
		self.operation_raw(&mut transaction_table, tid, operation, before, after)
	}

	pub fn commit(&self, tid: u64) -> Result<(), Error> {
		let mut transaction_table = self.transaction_table.lock();
		self.operation_raw(&mut transaction_table, tid, Operation::Commit, &[], &[])?;
		transaction_table.remove(&tid);
		Ok(())
	}

	pub fn assert_valid_tid(&self, tid: u64) -> Result<(), Error> {
		if !self.transaction_table.lock().contains_key(&tid) {
			return Err(Error::NoSuchTransaction(tid));
		}
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

struct TransactionTableRow {
	last_seq: u64,
	tid: u64,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn simple_transaction() {
		let tm = TransactionManager::new();
		let tid = tm.begin().unwrap();
		tm.operation(tid, Operation::Write(PageId::new(0, 1)), &[], &[])
			.unwrap();
		tm.commit(tid).unwrap();
	}

	#[test]
	fn try_operation_on_invalid_tid() {
		let tm = TransactionManager::new();
		assert!(tm
			.operation(69, Operation::Write(PageId::new(0, 1)), &[], &[])
			.is_err());
	}

	#[test]
	fn try_commit_invalid_tid() {
		let tm = TransactionManager::new();
		assert!(tm.commit(69).is_err());
	}
}
