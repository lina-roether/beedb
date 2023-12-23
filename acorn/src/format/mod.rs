use std::io;
use thiserror::Error;

pub use meta::*;
pub use state::*;

mod meta;
mod state;

const MAGIC: [u8; 4] = *b"ACRN";

#[derive(Debug, Error)]
pub enum Error {
	#[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
	NotAStorageFile,

	#[error("The format version {0} is not supported in this version of acorn")]
	UnsupportedVersion(u8),

	#[error("The storage is corrupted (Unexpected end of file)")]
	UnexpectedEOF,

	#[error("Failed to expand storage file")]
	IncompleteWrite,

	#[error("The storage file metadata is corrupted")]
	CorruptedMeta,

	#[error(transparent)]
	Io(#[from] io::Error),
}
