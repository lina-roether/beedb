use thiserror::Error;

use crate::files::FileError;

mod physical;

#[derive(Debug, Error)]
pub(crate) enum StorageError {
	#[error(transparent)]
	File(FileError),
}
