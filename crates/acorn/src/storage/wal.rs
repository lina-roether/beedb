use std::{
	borrow::Cow,
	collections::{HashMap, VecDeque},
	mem,
	sync::{
		atomic::{AtomicUsize, Ordering},
		Arc,
	},
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use static_assertions::assert_impl_all;

use crate::files::{
	wal::{self, CheckpointData, WalFileApi},
	DatabaseFolder, DatabaseFolderApi,
};

pub(super) use crate::files::wal::Item;

use super::{physical::PhysicalStorageApi, PageId, StorageError, WalIndex};

pub(super) struct Wal<DF: DatabaseFolderApi = DatabaseFolder> {
	folder: Arc<DF>,
	generations: RwLock<GenerationQueue<DF>>,
	state: Mutex<State>,
}
assert_impl_all!(Wal: Send, Sync);

impl<DF: DatabaseFolderApi> Wal<DF> {
	pub fn create(folder: Arc<DF>) -> Result<Self, StorageError> {
		folder.clear_wal_files()?;
		let mut gens: GenerationQueue<DF> = GenerationQueue::new();
		gens.push_generation(0, folder.open_wal_file(0)?);
		Self::write_checkpoint(&gens, &HashMap::new(), &HashMap::new())?;

		Ok(Self::new(folder, gens))
	}

	pub fn open(folder: Arc<DF>) -> Result<Self, StorageError> {
		let mut wal_files: Vec<(u64, DF::WalFile)> = Result::from_iter(folder.iter_wal_files()?)?;
		wal_files.sort_by(|(gen_1, _), (gen_2, _)| u64::cmp(gen_1, gen_2));

		let mut gens: GenerationQueue<DF> = GenerationQueue::new();
		for (gen, file) in wal_files {
			gens.push_generation(gen, file);
		}

		Ok(Self::new(folder, gens))
	}

	fn new(folder: Arc<DF>, generations: GenerationQueue<DF>) -> Self {
		Self {
			folder,
			generations: RwLock::new(generations),
			state: Mutex::new(State::default()),
		}
	}

	fn write_checkpoint(
		generations: &GenerationQueue<DF>,
		dirty_pages: &HashMap<PageId, WalIndex>,
		transactions: &HashMap<u64, WalIndex>,
	) -> Result<(), StorageError> {
		let Some(mut wal_file) = generations.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		wal_file.push_item(Item::Checkpoint(CheckpointData {
			dirty_pages: Cow::Borrowed(dirty_pages),
			transactions: Cow::Borrowed(transactions),
		}))?;

		Ok(())
	}

	fn cleanup_generations(
		&self,
		generations: &mut GenerationQueue<DF>,
	) -> Result<(), StorageError> {
		let mut delete_gens: Vec<u64> = Vec::new();
		for gen in &generations.generations {
			if gen.num_transactions.load(Ordering::Relaxed) != 0 {
				break;
			}
			delete_gens.push(gen.generation_num);
		}
		for gen_num in delete_gens {
			generations.generations.pop_front();
			self.folder.delete_wal_file(gen_num)?;
		}
		Ok(())
	}

	fn recover(
		file: &mut impl WalFileApi,
		generations: &GenerationQueue<DF>,
		physical_storage: &impl PhysicalStorageApi,
	) -> Result<(), StorageError> {
		let mut fuzzy_buffer: Vec<wal::Item> = Vec::new();
		let item_iter = file.iter_items()?;

		let mut checkpoint_data: Option<wal::CheckpointData> = None;
		for item_result in item_iter {
			match item_result? {
				wal::Item::Checkpoint(data) => {
					checkpoint_data = Some(data);
					break;
				}
				item => fuzzy_buffer.push(item),
			}
		}
		let mut state = match checkpoint_data {
			Some(data) => State {
				dirty_pages: data.dirty_pages.into_owned(),
				transactions: data.transactions.into_owned(),
			},
			None => State::default(),
		};

		for item in fuzzy_buffer {
			todo!()
		}

		todo!()
	}
}

pub(super) trait WalApi {
	fn push_item(&self, item: Item) -> Result<(), StorageError>;

	fn undo(
		&self,
		physical_storage: &impl PhysicalStorageApi,
		transaction_id: u64,
	) -> Result<(), StorageError>;

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

	fn checkpoint(&self) -> Result<(), StorageError> {
		// Acquire exclusive generations lock
		let mut generations_mut = self.generations.write();

		// Clone checkpoint-relevant state to ensure consistency
		// TODO: This might not be necessary, need to look into how the threads
		// interplay here
		let state = self.state.lock().clone();

		// Create the new generation and save it to the state object
		let gen_num = generations_mut.current_gen_num + 1;
		let file = self.folder.open_wal_file(gen_num)?;
		generations_mut.push_generation(gen_num, file);

		// Delete old generations if possible
		self.cleanup_generations(&mut generations_mut)?;

		// Release exclusive generations lock
		mem::drop(generations_mut);

		// Write checkpoint item to new generation
		let generations = self.generations.read();
		Self::write_checkpoint(&generations, &state.dirty_pages, &state.transactions)?;

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

struct GenerationQueue<DF: DatabaseFolderApi> {
	generations: VecDeque<WalGeneration<DF>>,
	current_gen_num: u64,
}

impl<DF: DatabaseFolderApi> GenerationQueue<DF> {
	fn new() -> Self {
		Self {
			generations: VecDeque::new(),
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

	fn track_transaction(&self) {
		let Some(generation) = self.generations.front() else {
			return;
		};
		generation.num_transactions.fetch_add(1, Ordering::AcqRel);
	}

	fn complete_transaction(&self, generation_num: u64) {
		for generation in &self.generations {
			if generation.generation_num == generation_num {
				generation.num_transactions.fetch_sub(1, Ordering::AcqRel);
			}
		}
	}
}

#[derive(Debug, Clone, Default)]
struct State {
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
}

impl State {
	fn track_transaction(&mut self, index: WalIndex, data: &wal::TransactionData) {
		self.transactions.insert(data.transaction_id, index);
	}

	fn complete_transaction(&mut self, data: &wal::TransactionData) {
		self.transactions.remove(&data.transaction_id);
	}

	fn track_write(&mut self, index: WalIndex, data: &wal::WriteData) {
		self.track_transaction(index, &data.transaction_data);
		self.dirty_pages.entry(data.page_id).or_insert(index);
	}

	fn handle_item(&mut self, index: WalIndex, item: &wal::Item) {
		match item {
			wal::Item::Write(data) => self.track_write(index, data),
			wal::Item::Commit(data) => self.complete_transaction(data),
			wal::Item::Undo(data) => self.complete_transaction(data),
			wal::Item::Checkpoint(..) => (),
		}
	}
}
