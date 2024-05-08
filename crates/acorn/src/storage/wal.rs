use std::{
	borrow::Cow,
	collections::{HashMap, VecDeque},
	mem,
	sync::{
		atomic::{AtomicU64, AtomicUsize, Ordering},
		Arc,
	},
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use static_assertions::assert_impl_all;

use crate::files::{
	wal::{CheckpointData, WalFileApi},
	DatabaseFolder, DatabaseFolderApi,
};

pub(super) use crate::files::wal::Item;

use super::{physical::PhysicalStorageApi, PageId, StorageError, WalIndex};

pub(super) struct Wal<DF: DatabaseFolderApi = DatabaseFolder> {
	folder: Arc<DF>,
	state: RwLock<State<DF>>,
}
assert_impl_all!(Wal: Send, Sync);

impl<DF: DatabaseFolderApi> Wal<DF> {
	pub fn create(folder: Arc<DF>) -> Result<Self, StorageError> {
		folder.clear_wal_files()?;
		let mut state: State<DF> = State::new();
		state.push_generation(0, folder.open_wal_file(0)?);
		state.write_checkpoint()?;

		Ok(Self::new(folder, state))
	}

	pub fn open(folder: Arc<DF>) -> Result<Self, StorageError> {
		let mut wal_files: Vec<(u64, DF::WalFile)> = Result::from_iter(folder.iter_wal_files()?)?;
		wal_files.sort_by(|(gen_1, _), (gen_2, _)| u64::cmp(gen_1, gen_2));

		let mut state: State<DF> = State::new();
		for (gen, file) in wal_files {
			state.push_generation(gen, file);
		}

		Ok(Self::new(folder, state))
	}

	fn new(folder: Arc<DF>, state: State<DF>) -> Self {
		Self {
			folder,
			state: RwLock::new(state),
		}
	}
}

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
		let mut state_mut = self.state.write();
		let gen_num = state_mut.current_gen_num + 1;
		let file = self.folder.open_wal_file(gen_num)?;
		state_mut.push_generation(gen_num, file);
		state_mut.cleanup_generations(&self.folder)?;
		mem::drop(state_mut);

		let state = self.state.read();
		state.write_checkpoint()?;
		Ok(())
	}
}

struct WalGeneration<DF: DatabaseFolderApi> {
	generation_num: u64,
	num_transactions: AtomicUsize,
	file: Mutex<DF::WalFile>,
}

impl<DF: DatabaseFolderApi> WalGeneration<DF> {
	fn new(generation_num: u64, file: DF::WalFile) -> Self {
		Self {
			generation_num,
			file: Mutex::new(file),
			num_transactions: AtomicUsize::new(0),
		}
	}
}

struct State<DF: DatabaseFolderApi> {
	generations: VecDeque<WalGeneration<DF>>,
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
	current_gen_num: u64,
}

impl<DF: DatabaseFolderApi> State<DF> {
	fn new() -> Self {
		Self {
			generations: VecDeque::new(),
			dirty_pages: HashMap::new(),
			transactions: HashMap::new(),
			current_gen_num: 0,
		}
	}

	fn push_generation(&mut self, gen_num: u64, file: DF::WalFile) {
		self.current_gen_num = u64::max(self.current_gen_num, gen_num);
		self.generations
			.push_back(WalGeneration::new(gen_num, file))
	}

	fn current_generation(&self) -> Option<MutexGuard<DF::WalFile>> {
		let generation = self.generations.front()?;
		assert_eq!(generation.generation_num, self.current_gen_num);
		Some(generation.file.lock())
	}

	fn write_checkpoint(&self) -> Result<(), StorageError> {
		let Some(mut wal_file) = self.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		wal_file.push_item(Item::Checkpoint(CheckpointData {
			dirty_pages: Cow::Borrowed(&self.dirty_pages),
			transactions: Cow::Borrowed(&self.transactions),
		}))?;

		Ok(())
	}

	fn cleanup_generations(&mut self, folder: &DF) -> Result<(), StorageError> {
		let mut delete_gens: Vec<u64> = Vec::new();
		for gen in &self.generations {
			if gen.num_transactions.load(Ordering::Relaxed) != 0 {
				break;
			}
			delete_gens.push(gen.generation_num);
		}
		for gen_num in delete_gens {
			self.generations.pop_front();
			folder.delete_wal_file(gen_num)?;
		}
		Ok(())
	}
}
