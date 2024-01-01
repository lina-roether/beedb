use std::{
	collections::HashMap,
	mem,
	sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::{index::PageId, transaction};

#[repr(u8)]
pub enum Operation {
	Begin,
	Update(PageId),
	Create(PageId),
	Delete(PageId),
	Commit,
}

#[derive(Debug, Error)]
pub enum Error {
	#[error("Transaction with id {0} doesn't exist")]
	DoesntExist(u64),
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

impl TransactionManager {
	pub fn new() -> Self {
		Self {
			sequence_counter: AtomicU64::new(0),
			transaction_counter: AtomicU64::new(0),
			transaction_table: Mutex::new(HashMap::new()),
		}
	}

	pub fn begin(&self) -> u64 {
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
		self.operation_raw(&mut transaction_table, tid, Operation::Begin, &[], &[]);
		tid
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

	fn operation_raw(
		&self,
		transaction_table: &mut HashMap<u64, TransactionTableRow>,
		tid: u64,
		operation: Operation,
		before: &[u8],
		after: &[u8],
	) -> Result<(), Error> {
		let seq = self.next_seq();
		let Some(row) = transaction_table.get_mut(&tid) else {
			return Err(Error::DoesntExist(tid));
		};
		row.last_seq = seq;

		// Doesn't do anything at the moment
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
