use std::{collections::HashMap, sync::Arc};

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

	fn recover(&self) -> Result<(), StorageError>;

	fn checkpoint(&self) -> Result<(), StorageError>;
}

struct State<DF: DatabaseFolderApi> {
	generations: HashMap<u64, DF::WalFile>,
	dirty_pages: HashMap<PageId, WalIndex>,
	transactions: HashMap<u64, WalIndex>,
}

impl<DF: DatabaseFolderApi> State<DF> {
	fn new() -> Self {
		Self {
			generations: HashMap::new(),
			dirty_pages: HashMap::new(),
			transactions: HashMap::new(),
		}
	}
}
