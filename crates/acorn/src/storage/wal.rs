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
		wal::{self, CheckpointData, WalFileApi},
		DatabaseFolder, DatabaseFolderApi,
	},
};

use super::{PageId, StorageError, TransactionState, WalIndex};

pub(crate) struct WalConfig {
	pub max_generation_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteOp<'a> {
	index: WalIndex,
	page_id: PageId,
	offset: u16,
	buf: &'a [u8],
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

		let wal = Self::new(folder, config, gens, State::default());
		wal.log_checkpoint()?;

		Ok(wal)
	}

	pub fn open(folder: Arc<DF>, config: &WalConfig) -> Result<Self, StorageError> {
		let mut wal_files: Vec<(u64, DF::WalFile)> = Result::from_iter(folder.iter_wal_files()?)?;
		wal_files.sort_by(|(gen_1, _), (gen_2, _)| u64::cmp(gen_1, gen_2));

		let mut gens: GenerationQueue<DF> = GenerationQueue::new();
		for (gen, file) in wal_files {
			gens.push_generation(gen, file);
		}

		Ok(Self::new(folder, config, gens, State::default()))
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

	fn log_checkpoint(&self) -> Result<(), StorageError> {
		let generations = self.generations.read();
		let Some(mut wal_file) = generations.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};

		let state = self.state.lock();
		wal_file.push_item(wal::Item::Checkpoint(CheckpointData {
			dirty_pages: Cow::Borrowed(&state.dirty_pages),
			transactions: Cow::Borrowed(&state.transactions),
		}))?;

		Ok(())
	}

	fn cleanup_generations(&self, gens: &mut GenerationQueue<DF>) -> Result<(), StorageError> {
		let state = self.state.lock();
		let first_needed = state.first_needed_generation();
		mem::drop(state);

		let mut delete_gens: Vec<u64> = Vec::new();
		for gen in &gens.generations {
			if gen.gen_num >= first_needed || gen.gen_num == gens.current_gen_num {
				break;
			}
			delete_gens.push(gen.gen_num);
		}

		for gen_num in delete_gens {
			gens.generations.pop_front();
			self.folder.delete_wal_file(gen_num)?;
		}
		Ok(())
	}

	fn read_initial_state(&self, file: &mut DF::WalFile) -> Result<(), StorageError> {
		let mut checkpoint_data: Option<wal::CheckpointData> = None;
		for item_result in file.iter_items()? {
			if let (_, wal::Item::Checkpoint(data)) = item_result? {
				checkpoint_data = Some(data);
				break;
			}
		}

		let mut state = self.state.lock();
		*state = match checkpoint_data {
			Some(data) => State::new(
				data.dirty_pages.into_owned(),
				data.transactions.into_owned(),
			),
			None => State::default(),
		};
		Ok(())
	}

	fn recover_state(&self, file: &mut DF::WalFile, gen_num: u64) -> Result<(), StorageError> {
		let mut state = self.state.lock();
		for item_result in file.iter_items()? {
			let (offset, item) = item_result?;
			state.handle_item(WalIndex::new(gen_num, offset), &item);
		}
		Ok(())
	}

	fn redo_write(
		&self,
		index: WalIndex,
		data: wal::WriteData,
		mut handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		let state = self.state.lock();
		let Some(first_dirty_index) = state.dirty_pages.get(&data.page_id).copied() else {
			return Ok(());
		};
		mem::drop(state);

		if index < first_dirty_index {
			return Ok(());
		}

		handle(WriteOp {
			index,
			page_id: data.page_id,
			offset: data.offset,
			buf: data.to.borrow(),
		})?;

		Ok(())
	}

	fn redo(
		&self,
		file: &mut DF::WalFile,
		gen_num: u64,
		mut handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		for item_result in file.iter_items()? {
			let (offset, item) = item_result?;
			let index = WalIndex::new(gen_num, offset);

			if let wal::Item::Write(data) = item {
				self.redo_write(index, data, &mut handle)?;
			}
		}
		Ok(())
	}

	fn create_undo_log(write: wal::WriteData<'_>) -> Option<UndoLog<'_>> {
		let from_buf = write.from?;

		Some(UndoLog {
			transaction_id: write.transaction_data.transaction_id,
			page_id: write.page_id,
			offset: write.offset,
			to: from_buf,
		})
	}

	fn apply_undo_log(
		&self,
		log: UndoLog,
		gens: &GenerationQueue<DF>,
		mut handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<WalIndex, StorageError> {
		let index = self.log_undo(log.clone(), gens)?;

		handle(WriteOp {
			page_id: log.page_id,
			offset: log.offset,
			index,
			buf: &log.to,
		})?;
		Ok(index)
	}

	fn undo_all(
		&self,
		transaction_ids: &[u64],
		gens: &mut GenerationQueue<DF>,
		mut handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		if transaction_ids.is_empty() {
			return Ok(());
		}

		let state = self.state.lock();
		let lowest_index: WalIndex = transaction_ids
			.iter()
			.filter_map(|tid| state.transactions.get(tid).map(|ts| ts.last_index))
			.min()
			.unwrap();
		mem::drop(state);

		let mut compensation_items: Vec<UndoLog> = Vec::new();

		'gen_loop: for generation in gens.generations.iter().rev() {
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
					if let Some(compensation_item) = Self::create_undo_log(data) {
						compensation_items.push(compensation_item);
					}
				}
			}
		}

		for item in compensation_items {
			self.apply_undo_log(item, gens, &mut handle)?;
		}

		for tid in transaction_ids {
			self.push_raw_item(wal::Item::Commit(self.create_transaction_data(*tid)), gens)?;

			let mut state = self.state.lock();
			state.complete_transaction(*tid);
			mem::drop(state);
		}

		Ok(())
	}

	fn push_raw_item(
		&self,
		item: wal::Item,
		gens: &GenerationQueue<DF>,
	) -> Result<WalIndex, StorageError> {
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

	fn log_undo(
		&self,
		undo_log: UndoLog,
		gens: &GenerationQueue<DF>,
	) -> Result<WalIndex, StorageError> {
		let write_data = self.create_undo_write_data(undo_log);
		self.push_raw_item(wal::Item::Write(write_data), gens)
	}
}

pub(super) trait WalApi {
	fn log_write(&self, log: WriteLog) -> Result<WalIndex, StorageError>;

	fn log_commit(&self, log: CommitLog) -> Result<WalIndex, StorageError>;

	fn undo(
		&self,
		transaction_id: u64,
		handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError>;

	fn recover(
		&self,
		handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError>;

	fn checkpoint(&self) -> Result<(), StorageError>;

	fn did_flush(&self);
}

impl<DF: DatabaseFolderApi> WalApi for Wal<DF> {
	fn log_write(&self, log: WriteLog) -> Result<WalIndex, StorageError> {
		let write_data = self.create_write_data(log);
		let gens = self.generations.read();
		self.push_raw_item(wal::Item::Write(write_data), &gens)
	}

	fn log_commit(&self, log: CommitLog) -> Result<WalIndex, StorageError> {
		let transaction_data = self.create_transaction_data(log.transaction_id);
		let gens = self.generations.read();
		self.push_raw_item(wal::Item::Commit(transaction_data), &gens)
	}

	fn undo(
		&self,
		transaction_id: u64,
		handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		let mut gens = self.generations.write();
		self.undo_all(&[transaction_id], &mut gens, handle)?;
		Ok(())
	}

	fn recover(
		&self,
		mut handle: impl FnMut(WriteOp) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		// acquire exclusive gen lock to prevent conflicts
		let mut gens = self.generations.write();

		let Some(mut file) = gens.current_generation() else {
			return Err(StorageError::WalNotInitialized);
		};

		self.read_initial_state(&mut file)?;
		self.recover_state(&mut file, gens.current_gen_num)?;
		self.redo(&mut file, gens.current_gen_num, &mut handle)?;
		mem::drop(file);

		let state = self.state.lock();
		let all_tids = state.transactions.keys().copied().collect::<Vec<_>>();
		mem::drop(state);

		self.undo_all(&all_tids, &mut gens, handle)?;

		Ok(())
	}

	fn checkpoint(&self) -> Result<(), StorageError> {
		// Acquire exclusive generations lock
		let mut generations_mut = self.generations.write();

		// Create the new generation and save it to the state object
		let gen_num = generations_mut.current_gen_num + 1;
		let file = self.folder.open_wal_file(gen_num)?;
		generations_mut.push_generation(gen_num, file);

		// Delete old generations if possible
		self.cleanup_generations(&mut generations_mut)?;

		// Release exclusive generations lock
		mem::drop(generations_mut);

		// Write checkpoint item to new generation
		self.log_checkpoint()?;

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
	use mockall::{predicate::*, Sequence};

	use crate::{
		files::MockDatabaseFolderApi,
		storage::{
			test_helpers::{page_id, wal_index},
			wal::tests::wal::test_helpers::mock_wal_file,
		},
		utils::test_helpers::{map, non_zero},
	};

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
					.returning(|_| Ok(non_zero!(69)));
				Ok(file)
			});

		// when
		Wal::create(Arc::new(folder), &WalConfig::default()).unwrap();
	}

	#[test]
	fn open_and_recover_wal() {
		// expect
		let mut folder = MockDatabaseFolderApi::new();
		folder.expect_iter_wal_files().returning(|| {
			//  WAL content

			// An older generation; has already been flushed to disk.
			let generation_2 = mock_wal_file! {
				// The initial checkpoint. Not relevant to this test case.
				10 => wal::Item::Checkpoint(wal::CheckpointData {
					transactions: Cow::Owned(HashMap::new()),
					dirty_pages: Cow::Owned(HashMap::new())
				}),

				// This write item was flushed to disk, but has no corresponding commit. It should
				// be reverted.
				20 => wal::Item::Write(wal::WriteData {
					transaction_data: wal::TransactionData {
						transaction_id: 1,
						prev_transaction_item: None
					},
					page_id: page_id!(100, 200),
					offset: 25,
					from: Some(vec![2, 2, 2, 2].into()),
					to: vec![1, 2, 3, 4].into()
				})
			};

			// The current generation; has likely not yet been flushed to disk.
			let mut generation_3 = mock_wal_file! {
				// This write item has a corresponding commit, but wasn't yet flushed to disk;
				// It should be reapplied.
				10 => wal::Item::Write(wal::WriteData {
					transaction_data: wal::TransactionData {
						transaction_id: 2,
						prev_transaction_item: None
					},
					page_id: page_id!(25, 69),
					offset: 100,
					from: Some(vec![0, 0, 0, 0].into()),
					to: vec![1, 2, 3, 4].into()
				}),

				// The checkpoint for gen 3. The preceding fuzzy write item should be handled
				// properly.
				20 => wal::Item::Checkpoint(wal::CheckpointData {
					transactions: Cow::Owned(map! {
						1 => TransactionState {
							first_gen: 2,
							last_index: wal_index!(2, 20)
						}
					}),
					dirty_pages: Cow::Owned(map! {
						page_id!(100, 200) => wal_index!(2, 20)
					})
				}),

				// The commit item for the write item at offset 10.
				30 => wal::Item::Commit(wal::TransactionData {
					transaction_id: 2,
					prev_transaction_item: Some(wal_index!(2, 30))
				})
			};

			let mut seq = Sequence::new();

			// Revert the uncommitted WAL item

			// 1. get the next offset
			generation_3
				.expect_next_offset()
				.once()
				.in_sequence(&mut seq)
				.returning(|| non_zero!(40));

			// 2. push the item
			generation_3
				.expect_push_item()
				.withf(|item| {
					item == &wal::Item::Write(wal::WriteData {
						transaction_data: wal::TransactionData {
							transaction_id: 1,
							prev_transaction_item: Some(WalIndex::new(2, non_zero!(20))),
						},
						page_id: page_id!(100, 200),
						offset: 25,
						from: None,
						to: Cow::Owned(vec![2, 2, 2, 2]),
					})
				})
				.once()
				.in_sequence(&mut seq)
				.returning(|_| Ok(non_zero!(40)));

			// 3. check the WAL file size
			generation_3
				.expect_size()
				.once()
				.in_sequence(&mut seq)
				.returning(|| 69420);

			// Write the commit item that marks the transaction as completed

			// 1. get the next offset
			generation_3
				.expect_next_offset()
				.once()
				.in_sequence(&mut seq)
				.returning(|| non_zero!(50));

			// 2. push the item
			generation_3
				.expect_push_item()
				.withf(|item| {
					item == &wal::Item::Commit(wal::TransactionData {
						transaction_id: 1,
						prev_transaction_item: Some(wal_index!(3, 40)),
					})
				})
				.once()
				.in_sequence(&mut seq)
				.returning(|_| Ok(non_zero!(50)));

			// 3. check the WAL file size
			generation_3
				.expect_size()
				.once()
				.in_sequence(&mut seq)
				.returning(|| 69420);

			Ok(vec![Ok((2, generation_2)), Ok((3, generation_3))].into_iter())
		});

		// when
		let mut expected_ops = vec![
			// This reapplies write (3, 10).
			WriteOp {
				index: wal_index!(3, 10),
				page_id: page_id!(25, 69),
				offset: 100,
				buf: &[1, 2, 3, 4],
			},
			// This reverts write (2, 20).
			WriteOp {
				index: wal_index!(3, 40),
				page_id: page_id!(100, 200),
				offset: 25,
				buf: &[2, 2, 2, 2],
			},
		]
		.into_iter();

		let wal = Wal::open(Arc::new(folder), &WalConfig::default()).unwrap();
		wal.recover(|op| {
			// Write operations should appear in the order of expected_ops.
			assert_eq!(Some(op), expected_ops.next());
			Ok(())
		})
		.unwrap();
	}
}
