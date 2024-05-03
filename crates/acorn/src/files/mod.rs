use std::{convert::Infallible, io};

use thiserror::Error;

use self::generic::FileType;

pub(super) mod generic;
pub(super) mod utils;
pub(crate) mod wal;

#[derive(Debug, Error)]
pub(crate) enum FileError {
	#[error("The file is not an acorn database file")]
	MissingMagic,

	#[error("The file was created on a platform with a different byte order and cannot be opened")]
	ByteOrderMismatch,

	#[error("The file is corrupted: {0}")]
	Corrupted(String),

	#[error("Unexpected file type {0:?}")]
	WrongFileType(FileType),

	#[error("Unexpected end of file")]
	UnexpectedEof,

	#[error("The file is corrupted; a checksum mismatch occurred")]
	ChecksumMismatch,

	#[error(transparent)]
	Io(#[from] io::Error),
}

impl From<Infallible> for FileError {
	fn from(value: Infallible) -> Self {
		match value {}
	}
}
