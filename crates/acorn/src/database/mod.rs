use document::SchemaError;
use thiserror::Error;

use crate::storage::{PageId, StorageError};

mod alloc;
mod document;
mod pages;

#[derive(Debug, Error)]
pub(crate) enum DatabaseError {
	#[error("Page format error on page {0}: {1}")]
	PageFormat(PageId, String),

	#[error(transparent)]
	Schema(#[from] SchemaError),

	#[error(transparent)]
	Storage(#[from] StorageError),
}
