use std::{
	collections::{HashMap, VecDeque},
	mem,
	sync::{
		atomic::{AtomicU64, Ordering},
		Arc,
	},
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use static_assertions::assert_impl_all;

use crate::files::{DatabaseFolder, DatabaseFolderApi};

pub(super) use crate::files::wal::Item;

use super::{physical::PhysicalStorageApi, PageId, StorageError, WalIndex};
use crate::files::wal::WalFileApi;

pub(super) struct Wal<DF: DatabaseFolderApi = DatabaseFolder> {
	folder: Arc<DF>,
	state: RwLock<State<DF>>,
	next_generation: AtomicU64,
}
assert_impl_all!(Wal: Send, Sync);

pub(super) trait WalApi {
	fn push_item(&self, item: Item) -> Result<(), StorageError>;

	fn undo(
		&self,
		physical_storage: &impl PhysicalStorageApi,
		transaction_id: u64,
	) -> Result<(), StorageError>;

	fn recover(&self, physical_storage: &impl PhysicalStorageApi) -> Result<(), StorageError>;

	fn checkpoint(&self) -> Result<(), StorageError>;
}

impl<DF: DatabaseFolderApi> WalApi for Wal<DF> {
	fn push_item(&self, item: Item) -> Result<(), StorageError> {
		let state = self.state.read();
		if let Some(mut gen) = state.lock_current_generation() {
			gen.file.push_item(item)?;
			return Ok(());
		}
		mem::drop(state);

		let mut state_mut = self.state.write();
		let gen_num = self.next_generation.load(Ordering::Acquire);
		let mut gen = state_mut.add_new_generation(gen_num, &self.folder)?;
		self.next_generation.store(gen_num + 1, Ordering::Release);
		gen.file.push_item(item)?;
		Ok(())
	}

	fn undo(
		&self,
		physical_storage: &impl PhysicalStorageApi,
		transaction_id: u64,
	) -> Result<(), StorageError> {
		todo!()
	}

	fn recover(&self, physical_storage: &impl PhysicalStorageApi) -> Result<(), StorageError> {
		todo!()
	}

	fn checkpoint(&self) -> Result<(), StorageError> {
		todo!()
	}
}

struct WalGeneration<DF: DatabaseFolderApi> {
	generation_num: u64,
	num_transactions: usize,
	file: DF::WalFile,
}

impl<DF: DatabaseFolderApi> WalGeneration<DF> {
	fn new(generation_num: u64, file: DF::WalFile) -> Self {
		Self {
			generation_num,
			file,
			num_transactions: 0,
		}
	}
}

struct State<DF: DatabaseFolderApi> {
	generations: VecDeque<Mutex<WalGeneration<DF>>>,
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
}

impl<DF: DatabaseFolderApi> State<DF> {
	fn new() -> Self {
		Self {
			generations: VecDeque::new(),
			dirty_pages: HashMap::new(),
			transactions: HashMap::new(),
		}
	}

	fn can_cleanup_generations(&self) -> bool {
		if let Some(oldest_generation) = self.generations.back() {
			oldest_generation.lock().num_transactions == 0
		} else {
			false
		}
	}

	fn cleanup_generations(&mut self, folder: &DF) -> Result<(), StorageError> {
		while let Some(generation_mutex) = self.generations.back() {
			let generation = generation_mutex.lock();
			if generation.num_transactions == 0 {
				let gen_num = generation.generation_num;
				mem::drop(generation);

				self.generations.pop_back();
				folder.delete_wal_file(gen_num)?;
			} else {
				break;
			}
		}
		Ok(())
	}

	fn lock_current_generation(&self) -> Option<MutexGuard<WalGeneration<DF>>> {
		self.generations.front().map(Mutex::lock)
	}

	fn add_new_generation(
		&mut self,
		generation: u64,
		folder: &DF,
	) -> Result<MutexGuard<WalGeneration<DF>>, StorageError> {
		let file = folder.open_wal_file(generation)?;
		self.generations
			.push_front(Mutex::new(WalGeneration::new(generation, file)));

		Ok(self.lock_current_generation().unwrap())
	}
}
