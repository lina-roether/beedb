use std::{
	borrow::{Borrow, Cow},
	collections::{hash_map::Entry, HashMap, VecDeque},
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

use super::{physical::PhysicalStorageApi, PageId, StorageError, TransactionState, WalIndex};

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

#[derive(Debug, Clone)]
pub(crate) struct WriteLog<'a> {
	pub transaction_id: u64,
	pub page_id: PageId,
	pub offset: u16,
	pub from: &'a [u8],
	pub to: &'a [u8],
}

#[derive(Debug, Clone)]
struct UndoLog<'a> {
	transaction_id: u64,
	page_id: PageId,
	offset: u16,
	to: Cow<'a, [u8]>,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitLog {
	pub transaction_id: u64,
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
		Self::log_checkpoint(&gens, &HashMap::new(), &HashMap::new())?;

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
		Self::cleanup_generations(&folder, &state, &mut gens)?;

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

	fn log_checkpoint(
		generations: &GenerationQueue<DF>,
		dirty_pages: &HashMap<PageId, WalIndex>,
		transactions: &HashMap<u64, TransactionState>,
	) -> Result<(), StorageError> {
		let Some(mut wal_file) = generations.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		wal_file.push_item(wal::Item::Checkpoint(CheckpointData {
			dirty_pages: Cow::Borrowed(dirty_pages),
			transactions: Cow::Borrowed(transactions),
		}))?;

		Ok(())
	}

	fn cleanup_generations(
		folder: &DF,
		state: &State,
		generations: &mut GenerationQueue<DF>,
	) -> Result<(), StorageError> {
		let first_needed = state.first_needed_generation();
		let mut delete_gens: Vec<u64> = Vec::new();
		for gen in &generations.generations {
			if gen.gen_num >= first_needed || gen.gen_num == generations.current_gen_num {
				break;
			}
			delete_gens.push(gen.gen_num);
		}
		for gen_num in delete_gens {
			generations.generations.pop_front();
			folder.delete_wal_file(gen_num)?;
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

	fn create_undo_log<'a>(write: wal::WriteData<'a>) -> Option<UndoLog<'a>> {
		let Some(from_buf) = write.from else {
			return None;
		};

		Some(UndoLog {
			transaction_id: write.transaction_data.transaction_id,
			page_id: write.page_id,
			offset: write.offset,
			to: from_buf,
		})
	}

	fn apply_undo_log(
		wal_file: &mut DF::WalFile,
		log: UndoLog,
		gen_num: u64,
		storage: &impl PhysicalStorageApi,
	) -> Result<WalIndex, StorageError> {
		let mut page_buf = [0; PAGE_BODY_SIZE];
		storage.read(log.page_id, &mut page_buf)?;
		page_buf[log.offset as usize..log.offset as usize + log.to.len()].copy_from_slice(&log.to);

		let wal_offset = wal_file.push_item(wal::Item::Write(item))?;

		storage.write(page_id, &page_buf, WalIndex::new(gen_num, wal_offset))?;
		Ok(WalIndex::new(gen_num, wal_offset))
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

		let lowest_index: WalIndex = transaction_ids
			.iter()
			.filter_map(|tid| state.transactions.get(tid).map(|ts| ts.last_index))
			.min()
			.unwrap();

		let mut compensation_items: Vec<wal::WriteData> = Vec::new();

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
					if let Some(compensation_item) = Self::create_undo_item(state, data)? {
						compensation_items.push(compensation_item);
					}
				}
			}
		}

		let Some(mut wal_file) = gen_queue.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};
		for item in compensation_items {
			let index = Self::apply_undo(&mut wal_file, gen_queue.current_gen_num, item, storage)?;
		}
		for tid in transaction_ids {
			wal_file.push_item(wal::Item::Commit(wal::TransactionData {
				transaction_id: *tid,
				prev_transaction_item: todo!(),
			}))?;
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
		mem::drop(file);

		let all_tids = state.transactions.keys().copied().collect::<Vec<_>>();

		Self::undo_all(&mut state, &all_tids, generations, storage)?;

		Ok(state)
	}

	fn push_raw_item(&self, item: wal::Item) -> Result<WalIndex, StorageError> {
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

		Ok(index)
	}

	fn create_transaction_data(&self, transaction_id: u64) -> wal::TransactionData {
		let state = self.state.lock();
		wal::TransactionData {
			transaction_id,
			prev_transaction_item: state
				.transactions
				.get(&transaction_id)
				.map(|ts| ts.last_index),
		}
	}

	fn create_write_data<'a>(&self, write_log: WriteLog<'a>) -> wal::WriteData<'a> {
		let transaction_data = self.create_transaction_data(write_log.transaction_id);
		wal::WriteData {
			transaction_data,
			page_id: write_log.page_id,
			offset: write_log.offset,
			from: Some(Cow::Borrowed(write_log.from)),
			to: Cow::Borrowed(write_log.to),
		}
	}

	fn create_undo_write_data<'a>(&self, undo_log: UndoLog<'a>) -> wal::WriteData<'a> {
		let transaction_data = self.create_transaction_data(undo_log.transaction_id);
		wal::WriteData {
			transaction_data,
			page_id: undo_log.page_id,
			offset: undo_log.offset,
			from: None,
			to: undo_log.to,
		}
	}

	fn log_undo(&self, undo_log: UndoLog) -> Result<WalIndex, StorageError> {
		let write_data = self.create_undo_write_data(undo_log);
		self.push_raw_item(wal::Item::Write(write_data))
	}
}

pub(super) trait WalApi {
	fn log_write(&self, log: WriteLog) -> Result<WalIndex, StorageError>;

	fn log_commit(&self, log: CommitLog) -> Result<WalIndex, StorageError>;

	fn undo(
		&self,
		transaction_id: u64,
		physical_storage: &impl PhysicalStorageApi,
	) -> Result<(), StorageError>;

	fn checkpoint(&self) -> Result<(), StorageError>;

	fn did_flush(&self);
}

impl<DF: DatabaseFolderApi> WalApi for Wal<DF> {
	fn log_write(&self, log: WriteLog) -> Result<WalIndex, StorageError> {
		let write_data = self.create_write_data(log);
		self.push_raw_item(wal::Item::Write(write_data))
	}

	fn log_commit(&self, log: CommitLog) -> Result<WalIndex, StorageError> {
		let transaction_data = self.create_transaction_data(log.transaction_id);
		self.push_raw_item(wal::Item::Commit(transaction_data))
	}

	fn undo(
		&self,
		transaction_id: u64,
		physical_storage: &impl PhysicalStorageApi,
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
		Self::cleanup_generations(&self.folder, &state, &mut generations_mut)?;

		// Release exclusive generations lock
		mem::drop(generations_mut);

		// Write checkpoint item to new generation
		let generations = self.generations.read();
		Self::log_checkpoint(&generations, &state.dirty_pages, &state.transactions)?;

		Ok(())
	}

	fn did_flush(&self) {
		let mut state = self.state.lock();
		state.did_flush();
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
		let generation = self.generations.back()?;
		assert_eq!(generation.gen_num, self.current_gen_num);
		Some(generation.file.lock())
	}
}

#[derive(Debug, Clone, Default)]
struct State {
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, TransactionState>,
}

impl State {
	fn new(
		dirty_pages: HashMap<PageId, WalIndex>,
		transactions: HashMap<u64, TransactionState>,
	) -> Self {
		Self {
			dirty_pages,
			transactions,
		}
	}

	fn track_transaction(&mut self, index: WalIndex, transaction_id: u64) {
		match self.transactions.entry(transaction_id) {
			Entry::Vacant(entry) => {
				entry.insert(TransactionState {
					first_gen: index.generation,
					last_index: index,
				});
			}
			Entry::Occupied(mut entry) => {
				entry.get_mut().last_index = index;
			}
		}
	}

	fn complete_transaction(&mut self, transaction_id: u64) {
		self.transactions.remove(&transaction_id);
	}

	fn track_write(&mut self, index: WalIndex, data: &wal::WriteData) {
		self.track_transaction(index, data.transaction_data.transaction_id);
		self.dirty_pages.entry(data.page_id).or_insert(index);
	}

	fn did_flush(&mut self) {
		self.dirty_pages.clear();
	}

	fn first_needed_generation(&self) -> u64 {
		self.transactions
			.values()
			.map(|ts| ts.first_gen)
			.min()
			.unwrap_or(u64::MAX)
	}

	fn handle_item(&mut self, index: WalIndex, item: &wal::Item) {
		match item {
			wal::Item::Write(data) => self.track_write(index, data),
			wal::Item::Commit(data) => self.complete_transaction(data.transaction_id),
			wal::Item::Checkpoint(..) => (),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::num::{NonZeroU16, NonZeroU64};

	use mockall::{predicate::*, Sequence};

	use crate::{files::MockDatabaseFolderApi, storage::physical::MockPhysicalStorageApi};

	use self::wal::MockWalFileApi;

	use super::*;

	#[test]
	fn create_wal() {
		// expect
		let mut folder = MockDatabaseFolderApi::new();
		let mut seq = Sequence::new();
		folder
			.expect_clear_wal_files()
			.once()
			.in_sequence(&mut seq)
			.returning(|| Ok(()));
		folder
			.expect_open_wal_file()
			.once()
			.in_sequence(&mut seq)
			.with(eq(0))
			.returning(|_| {
				let mut file = MockWalFileApi::new();
				file.expect_push_item()
					.once()
					.withf(|item| {
						item == &wal::Item::Checkpoint(CheckpointData {
							transactions: Cow::Owned(HashMap::new()),
							dirty_pages: Cow::Owned(HashMap::new()),
						})
					})
					.returning(|_| Ok(NonZeroU64::new(69).unwrap()));
				Ok(file)
			});

		// when
		Wal::create(Arc::new(folder), &WalConfig::default()).unwrap();
	}

	#[test]
	fn open_and_recover_wal() {
		// helpers
		fn generation_2_items() -> Vec<wal::Item<'static>> {
			vec![
				wal::Item::Checkpoint(wal::CheckpointData {
					transactions: Cow::Owned(HashMap::new()),
					dirty_pages: Cow::Owned(HashMap::new()),
				}),
				wal::Item::Write(wal::WriteData {
					transaction_data: wal::TransactionData {
						transaction_id: 1,
						prev_transaction_item: None,
					},
					page_id: PageId::new(100, NonZeroU16::new(200).unwrap()),
					offset: 25,
					from: Some(Cow::Owned(vec![2, 2, 2, 2])),
					to: Cow::Owned(vec![1, 2, 3, 4]),
				}),
			]
		}

		fn generation_3_items() -> Vec<wal::Item<'static>> {
			vec![
				wal::Item::Write(wal::WriteData {
					transaction_data: wal::TransactionData {
						transaction_id: 2,
						prev_transaction_item: None,
					},
					page_id: PageId::new(25, NonZeroU16::new(69).unwrap()),
					offset: 100,
					from: Some(Cow::Owned(vec![0, 0, 0, 0])),
					to: Cow::Owned(vec![1, 2, 3, 4]),
				}),
				wal::Item::Checkpoint(wal::CheckpointData {
					transactions: Cow::Owned({
						let mut map = HashMap::new();
						map.insert(
							1,
							TransactionState {
								first_gen: 2,
								last_index: WalIndex::new(2, NonZeroU64::new(20).unwrap()),
							},
						);
						map
					}),
					dirty_pages: Cow::Owned({
						let mut map = HashMap::new();
						map.insert(
							PageId::new(100, NonZeroU16::new(200).unwrap()),
							WalIndex::new(2, NonZeroU64::new(20).unwrap()),
						);
						map
					}),
				}),
				wal::Item::Commit(wal::TransactionData {
					transaction_id: 2,
					prev_transaction_item: Some(WalIndex::new(2, NonZeroU64::new(30).unwrap())),
				}),
			]
		}

		// expect
		let mut folder = MockDatabaseFolderApi::new();
		folder.expect_iter_wal_files().returning(|| {
			let mut generation_2 = MockWalFileApi::new();
			generation_2.expect_iter_items().returning(|| {
				let items = generation_2_items();
				Ok(vec![
					Ok((NonZeroU64::new(10).unwrap(), items[0].clone())),
					Ok((NonZeroU64::new(20).unwrap(), items[1].clone())),
				]
				.into_iter())
			});
			generation_2.expect_iter_items_reverse().returning(|| {
				let items = generation_2_items();
				Ok(vec![
					Ok((NonZeroU64::new(20).unwrap(), items[1].clone())),
					Ok((NonZeroU64::new(10).unwrap(), items[0].clone())),
				]
				.into_iter())
			});
			let mut generation_3 = MockWalFileApi::new();
			generation_3.expect_iter_items().returning(|| {
				let items = generation_3_items();
				Ok(vec![
					Ok((NonZeroU64::new(10).unwrap(), items[0].clone())),
					Ok((NonZeroU64::new(20).unwrap(), items[1].clone())),
					Ok((NonZeroU64::new(30).unwrap(), items[2].clone())),
				]
				.into_iter())
			});
			generation_3.expect_iter_items_reverse().returning(|| {
				let items = generation_3_items();
				Ok(vec![
					Ok((NonZeroU64::new(30).unwrap(), items[2].clone())),
					Ok((NonZeroU64::new(20).unwrap(), items[1].clone())),
					Ok((NonZeroU64::new(10).unwrap(), items[0].clone())),
				]
				.into_iter())
			});

			let mut seq = Sequence::new();
			generation_3
				.expect_push_item()
				.withf(|item| {
					item == &wal::Item::Write(wal::WriteData {
						transaction_data: wal::TransactionData {
							transaction_id: 1,
							prev_transaction_item: Some(WalIndex::new(
								2,
								NonZeroU64::new(20).unwrap(),
							)),
						},
						page_id: PageId::new(100, NonZeroU16::new(200).unwrap()),
						offset: 25,
						from: None,
						to: Cow::Owned(vec![2, 2, 2, 2]),
					})
				})
				.once()
				.in_sequence(&mut seq)
				.returning(|_| Ok(NonZeroU64::new(40).unwrap()));
			generation_3
				.expect_push_item()
				.withf(|item| {
					item == &wal::Item::Commit(wal::TransactionData {
						transaction_id: 1,
						prev_transaction_item: Some(WalIndex::new(3, NonZeroU64::new(40).unwrap())),
					})
				})
				.once()
				.in_sequence(&mut seq)
				.returning(|_| Ok(NonZeroU64::new(50).unwrap()));

			Ok(vec![Ok((2, generation_2)), Ok((3, generation_3))].into_iter())
		});
		let mut physical_storage = MockPhysicalStorageApi::new();
		let mut seq = Sequence::new();
		physical_storage
			.expect_read()
			.with(eq(PageId::new(25, NonZeroU16::new(69).unwrap())), always())
			.once()
			.in_sequence(&mut seq)
			.returning(|_, buf| {
				buf.fill(0);
				Ok(WalIndex::new(1, NonZeroU64::new(20).unwrap()))
			});
		physical_storage
			.expect_write()
			.with(
				eq(PageId::new(25, NonZeroU16::new(69).unwrap())),
				function(|buf: &[u8]| buf[100..104] == [1, 2, 3, 4]),
				eq(WalIndex::new(3, NonZeroU64::new(10).unwrap())),
			)
			.once()
			.in_sequence(&mut seq)
			.returning(|_, _, _| Ok(()));
		physical_storage
			.expect_read()
			.with(
				eq(PageId::new(100, NonZeroU16::new(200).unwrap())),
				always(),
			)
			.once()
			.in_sequence(&mut seq)
			.returning(|_, buf| {
				buf.fill(0);
				Ok(WalIndex::new(2, NonZeroU64::new(20).unwrap()))
			});
		physical_storage
			.expect_write()
			.with(
				eq(PageId::new(100, NonZeroU16::new(200).unwrap())),
				function(|buf: &[u8]| buf[25..29] == [2, 2, 2, 2]),
				eq(WalIndex::new(3, NonZeroU64::new(40).unwrap())),
			)
			.once()
			.in_sequence(&mut seq)
			.returning(|_, _, _| Ok(()));
		folder
			.expect_delete_wal_file()
			.with(eq(2))
			.once()
			.in_sequence(&mut seq)
			.returning(|_| Ok(()));

		// when
		Wal::open_and_recover(Arc::new(folder), &WalConfig::default(), &physical_storage).unwrap();
	}
}
