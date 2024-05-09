use std::{
	borrow::Cow,
	collections::{HashMap, VecDeque},
	mem,
	sync::Arc,
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
		state: &State,
		generations: &mut GenerationQueue<DF>,
	) -> Result<(), StorageError> {
		let mut delete_gens: Vec<u64> = Vec::new();
		for gen in &generations.generations {
			if state.transactions_in_generation(gen.gen_num) != 0 {
				break;
			}
			delete_gens.push(gen.gen_num);
		}
		for gen_num in delete_gens {
			generations.generations.pop_front();
			self.folder.delete_wal_file(gen_num)?;
		}
		Ok(())
	}

	fn read_initial_state(file: &mut impl WalFileApi) -> Result<State, StorageError> {
		let mut checkpoint_data: Option<wal::CheckpointData> = None;
		for item_result in file.iter_items()? {
			if let (_, wal::Item::Checkpoint(data)) = item_result? {
				checkpoint_data = Some(data);
				break;
			}
		}
		let state = match checkpoint_data {
			Some(data) => State::new(
				data.dirty_pages.into_owned(),
				data.transactions.into_owned(),
			),
			None => State::default(),
		};
		Ok(state)
	}

	fn recover_state(
		file: &mut impl WalFileApi,
		gen_num: u64,
		state: &mut State,
	) -> Result<(), StorageError> {
		for item_result in file.iter_items()? {
			let (offset, item) = item_result?;
			state.handle_item(WalIndex::new(gen_num, offset), &item);
		}
		Ok(())
	}

	fn redo(
		file: &mut impl WalFileApi,
		state: &State,
		gen_num: u64,
		storage: &impl PhysicalStorageApi,
	) -> Result<(), StorageError> {
		for item_result in file.iter_items()? {
			let (offset, item) = item_result?;
			let index = WalIndex::new(gen_num, offset);

			match item {
				wal::Item::Write(data) => {
					let Some(first_dirty_index) = state.dirty_pages.get(&data.page_id) else {
						continue;
					};
					if index < *first_dirty_index {
						continue;
					}
				}
				_ => todo!(),
			}
		}

		todo!()
	}

	fn recover(file: &mut impl WalFileApi, gen_num: u64) -> Result<(), StorageError> {
		let mut state = Self::read_initial_state(file)?;
		Self::recover_state(file, gen_num, &mut state)?;
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
		self.cleanup_generations(&state, &mut generations_mut)?;

		// Release exclusive generations lock
		mem::drop(generations_mut);

		// Write checkpoint item to new generation
		let generations = self.generations.read();
		Self::write_checkpoint(&generations, &state.dirty_pages, &state.transactions)?;

		Ok(())
	}
}

struct WalGeneration<DF: DatabaseFolderApi> {
	gen_num: u64,
	file: Mutex<DF::WalFile>,
}

impl<DF: DatabaseFolderApi> WalGeneration<DF> {
	fn new(generation_num: u64, file: DF::WalFile) -> Self {
		Self {
			gen_num: generation_num,
			file: Mutex::new(file),
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
		assert_eq!(generation.gen_num, self.current_gen_num);
		Some(generation.file.lock())
	}
}

#[derive(Debug, Clone, Default)]
struct State {
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
	transactions_per_generation: HashMap<u64, usize>,
}

impl State {
	fn new(dirty_pages: HashMap<PageId, WalIndex>, transactions: HashMap<u64, WalIndex>) -> Self {
		let mut transactions_per_generation: HashMap<u64, usize> = HashMap::new();
		for wal_index in transactions.values() {
			*transactions_per_generation
				.entry(wal_index.generation)
				.or_default() += 1;
		}
		Self {
			dirty_pages,
			transactions,
			transactions_per_generation,
		}
	}

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

	fn transactions_in_generation(&self, gen_num: u64) -> usize {
		self.transactions_per_generation
			.get(&gen_num)
			.copied()
			.unwrap_or_default()
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
