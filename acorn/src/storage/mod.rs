mod file;

pub use file::*;
use parking_lot::RwLock;
use thiserror::Error;

use crate::format::{self, Meta, PageStorage, State};

#[derive(Debug, Error)]
pub enum LoadError {
	#[error(transparent)]
	Format(#[from] format::Error),
}

pub struct Storage<F: StorageFile> {
	meta: Meta,
	state: RwLock<State>,
	pages: PageStorage<F>,
}
