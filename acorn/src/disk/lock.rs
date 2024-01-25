use std::{
	iter,
	sync::atomic::{AtomicU16, Ordering},
	usize,
};

use parking_lot::{lock_api::RawRwLock as _, RawRwLock, RwLock};
use static_assertions::assert_impl_all;

pub(super) struct PageLocker {
	locks: RwLock<Vec<RawRwLock>>,
	locks_len: AtomicU16,
}

assert_impl_all!(PageLocker: Send, Sync);

impl PageLocker {
	#[inline]
	pub fn new() -> Self {
		Self {
			locks_len: AtomicU16::new(0),
			locks: RwLock::new(Vec::new()),
		}
	}

	pub fn lock_shared(&self, page_num: u16) {
		self.ensure_has_lock(page_num);
		let locks = self.locks.read();
		locks[page_num as usize].lock_shared();
	}

	pub unsafe fn unlock_shared(&self, page_num: u16) {
		let locks = self.locks.read();
		locks[page_num as usize].unlock_shared();
	}

	pub fn lock_exclusive(&self, page_num: u16) {
		self.ensure_has_lock(page_num);
		let locks = self.locks.read();
		locks[page_num as usize].lock_exclusive();
	}

	pub unsafe fn unlock_exclusive(&self, page_num: u16) {
		let locks = self.locks.read();
		locks[page_num as usize].unlock_exclusive();
	}

	fn ensure_has_lock(&self, page_num: u16) {
		if self.locks_len.load(Ordering::Acquire) <= page_num {
			let new_length = page_num + 1;
			self.extend_until(new_length as usize);
			self.locks_len.store(new_length, Ordering::Release);
		}
	}

	fn extend_until(&self, new_length: usize) {
		let mut locks_mut = self.locks.write();
		let extend_by = new_length - locks_mut.len();
		locks_mut.extend(iter::repeat_with(|| RawRwLock::INIT).take(extend_by));
	}
}
