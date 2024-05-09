use std::{
	borrow::Cow,
	collections::{HashMap, VecDeque},
	mem,
	sync::Arc,
};

use parking_lot::{Mutex, MutexGuard, RwLock};
use static_assertions::assert_impl_all;

use crate::{
	consts::DEFAULT_MAX_WAL_GENERATION_SIZE,
	files::{
		segment::PAGE_BODY_SIZE,
		wal::{self, CheckpointData, WalFileApi},
		DatabaseFolder, DatabaseFolderApi,
	},
};

pub(super) use crate::files::wal::Item;

use super::{physical::PhysicalStorageApi, PageId, StorageError, WalIndex};

pub(crate) struct WalConfig {
	pub max_generation_size: usize,
}

impl Default for WalConfig {
	fn default() -> Self {
		Self {
			max_generation_size: DEFAULT_MAX_WAL_GENERATION_SIZE,
		}
	}
}

pub(super) struct Wal<DF: DatabaseFolderApi = DatabaseFolder> {
	folder: Arc<DF>,
	generations: RwLock<GenerationQueue<DF>>,
	state: Mutex<State>,
	max_generation_size: usize,
}
assert_impl_all!(Wal: Send, Sync);

impl<DF: DatabaseFolderApi> Wal<DF> {
	pub fn create(folder: Arc<DF>, config: &WalConfig) -> Result<Self, StorageError> {
		folder.clear_wal_files()?;
		let mut gens: GenerationQueue<DF> = GenerationQueue::new();
		gens.push_generation(0, folder.open_wal_file(0)?);
		Self::write_checkpoint(&gens, &HashMap::new(), &HashMap::new())?;

		Ok(Self::new(folder, config, gens, State::default()))
	}

	pub fn open_and_recover(
		folder: Arc<DF>,
		config: &WalConfig,
		storage: &impl PhysicalStorageApi,
	) -> Result<Self, StorageError> {
		let mut wal_files: Vec<(u64, DF::WalFile)> = Result::from_iter(folder.iter_wal_files()?)?;
		wal_files.sort_by(|(gen_1, _), (gen_2, _)| u64::cmp(gen_1, gen_2));

		let mut gens: GenerationQueue<DF> = GenerationQueue::new();
		for (gen, file) in wal_files {
			gens.push_generation(gen, file);
		}

		let state = Self::recover(&gens, storage)?;

		Ok(Self::new(folder, config, gens, state))
	}

	fn new(
		folder: Arc<DF>,
		config: &WalConfig,
		generations: GenerationQueue<DF>,
		state: State,
	) -> Self {
		Self {
			folder,
			generations: RwLock::new(generations),
			state: Mutex::new(state),
			max_generation_size: config.max_generation_size,
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

	fn redo_write(
		state: &State,
		index: WalIndex,
		data: wal::WriteData,
		storage: &impl PhysicalStorageApi,
	) -> Result<(), StorageError> {
		let Some(first_dirty_index) = state.dirty_pages.get(&data.page_id) else {
			return Ok(());
		};
		if index < *first_dirty_index {
			return Ok(());
		}
		let mut page_buf = [0; PAGE_BODY_SIZE];
		let last_written_index = storage.read(data.page_id, &mut page_buf)?;
		if index < last_written_index {
			return Ok(());
		}

		page_buf[data.offset.into()..data.offset as usize + data.to.len()]
			.copy_from_slice(&data.to);
		storage.write(data.page_id, &page_buf, index)?;
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

			if let wal::Item::Write(data) = item {
				Self::redo_write(state, index, data, storage)?
			}
		}
		Ok(())
	}

	fn undo_write<'a>(
		index: WalIndex,
		state: &State,
		data: wal::WriteData<'a>,
		storage: &impl PhysicalStorageApi,
	) -> Result<Option<wal::Item<'a>>, StorageError> {
		let Some(from_buf) = data.from else {
			return Ok(None);
		};

		let mut page_buf = [0; PAGE_BODY_SIZE];
		storage.read(data.page_id, &mut page_buf)?;

		page_buf[data.offset.into()..data.offset as usize + from_buf.len()]
			.copy_from_slice(&from_buf);
		storage.write(data.page_id, &page_buf, index)?;

		let transaction_id = data.transaction_data.transaction_id;
		let prev_transaction_item = state.transactions.get(&transaction_id).copied();

		Ok(Some(wal::Item::Write(wal::WriteData {
			transaction_data: wal::TransactionData {
				transaction_id,
				prev_transaction_item,
			},
			page_id: data.page_id,
			offset: data.offset,
			from: None,
			to: from_buf,
		})))
	}

	fn undo_all(
		state: &mut State,
		transaction_ids: &[u64],
		gen_queue: &GenerationQueue<DF>,
		storage: &impl PhysicalStorageApi,
	) -> Result<(), StorageError> {
		if transaction_ids.is_empty() {
			return Ok(());
		}

		let lowest_index: WalIndex = *transaction_ids
			.iter()
			.filter_map(|tid| state.transactions.get(tid))
			.min()
			.unwrap();

		let mut compensation_items: Vec<wal::Item> = Vec::new();

		'gen_loop: for generation in gen_queue.generations.iter().rev() {
			let mut wal_file = generation.file.lock();
			'item_loop: for item_result in wal_file.iter_items_reverse()? {
				let (offset, item) = item_result?;
				let index = WalIndex::new(generation.gen_num, offset);
				if index < lowest_index {
					break 'gen_loop;
				}

				if let wal::Item::Write(data) = item {
					if !transaction_ids.contains(&data.transaction_data.transaction_id) {
						continue 'item_loop;
					}
					if let Some(compensation_item) = Self::undo_write(index, state, data, storage)?
					{
						compensation_items.push(compensation_item);
					}
				}
			}
		}

		let Some(mut wal_file) = gen_queue.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		for item in compensation_items {
			wal_file.push_item(item)?;
		}
		for tid in transaction_ids {
			state.complete_transaction(*tid);
		}
		Ok(())
	}

	fn recover(
		generations: &GenerationQueue<DF>,
		storage: &impl PhysicalStorageApi,
	) -> Result<State, StorageError> {
		let Some(mut file) = generations.current_generation() else {
			return Ok(State::default());
		};

		let mut state = Self::read_initial_state(&mut *file)?;
		Self::recover_state(&mut *file, generations.current_gen_num, &mut state)?;
		Self::redo(&mut *file, &state, generations.current_gen_num, storage)?;

		let all_tids = state.transactions.keys().copied().collect::<Vec<_>>();

		Self::undo_all(&mut state, &all_tids, generations, storage)?;

		Ok(state)
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
		let gens = self.generations.read();
		let Some(mut wal_file) = gens.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		let index = WalIndex::new(gens.current_gen_num, wal_file.next_offset());

		let mut state = self.state.lock();
		state.handle_item(index, &item);
		mem::drop(state);

		wal_file.push_item(item)?;

		if wal_file.size() >= self.max_generation_size {
			mem::drop(wal_file);
			self.checkpoint()?;
		}

		Ok(())
	}

	fn undo(
		&self,
		physical_storage: &impl PhysicalStorageApi,
		transaction_id: u64,
	) -> Result<(), StorageError> {
		let gens = self.generations.read();
		let mut state = self.state.lock();

		Self::undo_all(&mut state, &[transaction_id], &gens, physical_storage)?;
		Ok(())
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

	fn track_transaction(&mut self, index: WalIndex, transaction_id: u64) {
		self.transactions.insert(transaction_id, index);
	}

	fn complete_transaction(&mut self, transaction_id: u64) {
		self.transactions.remove(&transaction_id);
	}

	fn track_write(&mut self, index: WalIndex, data: &wal::WriteData) {
		self.track_transaction(index, data.transaction_data.transaction_id);
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
			wal::Item::Commit(data) => self.complete_transaction(data.transaction_id),
			wal::Item::Checkpoint(..) => (),
		}
	}
}
