use std::{
	borrow::Cow,
	collections::{HashMap, VecDeque},
	sync::{
		atomic::{AtomicU64, AtomicUsize},
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
	current_generation: AtomicU64,
}
assert_impl_all!(Wal: Send, Sync);

impl<DF: DatabaseFolderApi> Wal<DF> {
	fn create(folder: Arc<DF>) -> Result<Self, StorageError> {
		folder.clear_wal_files()?;
		let mut state: State<DF> = State::new();
		state.add_generation(0, folder.open_wal_file(0)?);
		state.write_checkpoint()?;

		Ok(Self::new(folder, state, 0))
	}

	fn new(folder: Arc<DF>, state: State<DF>, current_generation: u64) -> Self {
		Self {
			folder,
			state: RwLock::new(state),
			current_generation: AtomicU64::new(current_generation),
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
		todo!()
	}
}

struct WalGeneration<DF: DatabaseFolderApi> {
	generation_num: AtomicU64,
	num_transactions: AtomicUsize,
	file: Mutex<DF::WalFile>,
}

impl<DF: DatabaseFolderApi> WalGeneration<DF> {
	fn new(generation_num: u64, file: DF::WalFile) -> Self {
		Self {
			generation_num: AtomicU64::new(generation_num),
			file: Mutex::new(file),
			num_transactions: AtomicUsize::new(0),
		}
	}
}

struct State<DF: DatabaseFolderApi> {
	generations: VecDeque<WalGeneration<DF>>,
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

	fn add_generation(&mut self, gen_num: u64, file: DF::WalFile) {
		self.generations
			.push_front(WalGeneration::new(gen_num, file))
	}

	fn current_generation(&self) -> Option<MutexGuard<DF::WalFile>> {
		self.generations.front().map(|gen| gen.file.lock())
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
}
