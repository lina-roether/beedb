use std::{
	iter,
	sync::atomic::{AtomicU32, Ordering},
	usize,
};

use parking_lot::{lock_api::RawRwLock as _, RawRwLock, RwLock};

#[must_use = "The page guard is dropped right after acquiring it"]
pub struct PageReadGuard<'a> {
	locker: &'a PageLocker,
	page_num: u32,
}

impl<'a> PageReadGuard<'a> {
	#[inline]
	pub fn page_number(&self) -> u32 {
		self.page_num
	}
}

impl<'a> Drop for PageReadGuard<'a> {
	fn drop(&mut self) {
		unsafe { self.locker.unlock_shared(self.page_number()) }
	}
}

#[must_use = "The page guard is dropped right after acquiring it"]
pub struct PageWriteGuard<'a> {
	locker: &'a PageLocker,
	page_num: u32,
}

impl<'a> PageWriteGuard<'a> {
	#[inline]
	pub fn page_number(&self) -> u32 {
		self.page_num
	}
}

impl<'a> Drop for PageWriteGuard<'a> {
	fn drop(&mut self) {
		unsafe { self.locker.unlock_exclusive(self.page_number()) }
	}
}

pub struct PageLocker {
	locks_len: AtomicU32,
	locks: RwLock<Vec<RawRwLock>>,
}

impl PageLocker {
	/// Creating a PageLocker is unsafe, because if there is more than one page
	/// locker, The exclusivity guarantees of PageWriteGuard etc. no longer hold
	#[inline]
	pub unsafe fn new() -> Self {
		Self {
			locks_len: AtomicU32::new(0),
			locks: RwLock::new(Vec::new()),
		}
	}

	pub fn read(&self, page_num: u32) -> PageReadGuard {
		self.lock_shared(page_num);
		PageReadGuard {
			page_num,
			locker: self,
		}
	}

	pub fn write(&self, page_num: u32) -> PageWriteGuard {
		self.lock_exclusive(page_num);
		PageWriteGuard {
			page_num,
			locker: self,
		}
	}

	fn lock_shared(&self, page_num: u32) {
		self.ensure_has_lock(page_num);
		let locks = self.locks.read();
		locks[page_num as usize].lock_shared();
	}

	unsafe fn unlock_shared(&self, page_num: u32) {
		let locks = self.locks.read();
		locks[page_num as usize].unlock_shared();
	}

	fn lock_exclusive(&self, page_num: u32) {
		self.ensure_has_lock(page_num);
		let locks = self.locks.read();
		locks[page_num as usize].lock_exclusive();
	}

	unsafe fn unlock_exclusive(&self, page_num: u32) {
		let locks = self.locks.read();
		locks[page_num as usize].unlock_exclusive();
	}

	fn ensure_has_lock(&self, page_num: u32) {
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
