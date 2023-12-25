use std::io;
use thiserror::Error;

use self::target::IoTarget;

mod format;
mod lock;
mod target;

const MAGIC: [u8; 4] = *b"ACRN";

#[derive(Debug, Error)]
pub enum StorageError {
	#[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
	NotAStorageFile,

	#[error("The format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("The storage is corrupted (Unexpected end of file)")]
	IncompleteRead,

	#[error("Failed to expand storage file")]
	IncompleteWrite,

	#[error("An error occurred accessing the storage file: {0}")]
	Io(#[from] io::Error),
}

pub struct StorageFile<T: IoTarget> {
	header_buf: Box<[u8]>,
	page_size: usize,
	target: T,
}
