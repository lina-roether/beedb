use std::io;

use thiserror::Error;

mod dir;

#[derive(Debug, Error)]
pub enum DataError {
	#[error("An error occurred accessing the data directory: {0}")]
	Io(#[from] io::Error),
}
