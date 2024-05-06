use std::{
	collections::{HashMap, VecDeque},
	sync::{
		atomic::{AtomicU64, AtomicUsize, Ordering},
		Arc,
	},
};

use parking_lot::RwLock;
use static_assertions::assert_impl_all;

use crate::files::{DatabaseFolder, DatabaseFolderApi};

pub(super) use crate::files::wal::Item;

use super::{physical::PhysicalStorageApi, PageId, StorageError, WalIndex};

pub(super) struct Wal<DF: DatabaseFolderApi = DatabaseFolder> {
	folder: Arc<DF>,
	state: RwLock<State<DF>>,
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
		todo!()
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
	num_transactions: AtomicUsize,
	file: DF::WalFile,
}

impl<DF: DatabaseFolderApi> WalGeneration<DF> {
	fn new(generation_num: u64, file: DF::WalFile) -> Self {
		Self {
			generation_num,
			file,
			num_transactions: AtomicUsize::new(0),
		}
	}

	fn track_transaction(&self) {
		self.num_transactions.fetch_add(1, Ordering::AcqRel);
	}

	fn untrack_transaction(&self) {
		self.num_transactions.fetch_sub(1, Ordering::AcqRel);
	}

	fn num_transactions(&self) -> usize {
		self.num_transactions.load(Ordering::Acquire)
	}
}

struct State<DF: DatabaseFolderApi> {
	generations: VecDeque<WalGeneration<DF>>,
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
	current_generation: AtomicU64,
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
			oldest_generation.num_transactions() == 0
		} else {
			false
		}
	}

	fn cleanup_generations(&mut self, folder: &DF) -> Result<(), StorageError> {
		while let Some(generation) = self.generations.back() {
			if generation.num_transactions() == 0 {
				let gen_num = generation.generation_num;
				self.generations.pop_back();
				folder.delete_wal_file(gen_num)?;
			} else {
				break;
			}
		}
		Ok(())
	}

	fn get_current_generation(&self) -> Option<&WalGeneration<DF>> {
		self.generations.front()
	}

	fn add_new_generation(
		&mut self,
		generation: u64,
		folder: &DF,
	) -> Result<&WalGeneration<DF>, StorageError> {
		let file = folder.open_wal_file(generation)?;
		self.generations
			.push_front(WalGeneration::new(generation, file));

		Ok(self.generations.front().unwrap())
	}
}
