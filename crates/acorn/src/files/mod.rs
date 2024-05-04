use std::{convert::Infallible, fs, io, path::PathBuf};

use thiserror::Error;

#[cfg(test)]
use mockall::automock;

use self::{
	generic::FileType,
	segment::{SegmentFile, SegmentFileApi},
	wal::{WalFile, WalFileApi},
};

#[cfg(test)]
use self::{segment::MockSegmentFileApi, wal::MockWalFileApi};

pub(super) mod generic;
pub(crate) mod segment;
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

	#[error("Incompatible version of {0:?} file: {1}")]
	IncompatibleVersion(FileType, u8),

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

pub(crate) struct DatabaseFolder {
	path: PathBuf,
}

impl DatabaseFolder {
	const SEGMENTS_DIR_NAME: &'static str = "segments";
	const WAL_DIR_NAME: &'static str = "wal";

	pub fn open(path: PathBuf) -> Self {
		Self { path }
	}

	fn segments_dir(&self) -> Result<PathBuf, FileError> {
		let path = self.path.join(Self::SEGMENTS_DIR_NAME);
		fs::create_dir_all(&path)?;
		Ok(path)
	}

	fn segment_file_path(&self, segment_id: u32) -> Result<PathBuf, FileError> {
		self.segments_dir().map(|p| p.join(segment_id.to_string()))
	}

	fn wal_dir(&self) -> Result<PathBuf, FileError> {
		let path = self.path.join(Self::WAL_DIR_NAME);
		fs::create_dir_all(&path)?;
		Ok(path)
	}

	fn wal_file_path(&self, generation: u64) -> Result<PathBuf, FileError> {
		self.wal_dir().map(|p| p.join(generation.to_string()))
	}
}

#[cfg_attr(test, automock(
    type SegmentFile = MockSegmentFileApi;
    type WalFile = MockWalFileApi;
))]
pub(crate) trait DatabaseFolderApi {
	type SegmentFile: SegmentFileApi;
	type WalFile: WalFileApi;

	fn create_segment_file(&self, segment_id: u32) -> Result<Self::SegmentFile, FileError>;
	fn open_segment_file(&self, segment_id: u32) -> Result<Option<Self::SegmentFile>, FileError>;
	fn create_wal_file(&self, generation: u64) -> Result<Self::WalFile, FileError>;
	fn open_wal_file(&self, generation: u64) -> Result<Option<Self::WalFile>, FileError>;
}

impl DatabaseFolderApi for DatabaseFolder {
	type SegmentFile = SegmentFile;
	type WalFile = WalFile;

	fn create_segment_file(&self, segment_id: u32) -> Result<Self::SegmentFile, FileError> {
		SegmentFile::create_file(self.segment_file_path(segment_id)?)
	}

	fn open_segment_file(&self, segment_id: u32) -> Result<Option<Self::SegmentFile>, FileError> {
		let path = self.segment_file_path(segment_id)?;
		if path.exists() {
			let file = SegmentFile::open_file(path)?;
			Ok(Some(file))
		} else {
			Ok(None)
		}
	}

	fn create_wal_file(&self, generation: u64) -> Result<Self::WalFile, FileError> {
		WalFile::create_file(self.wal_file_path(generation)?)
	}

	fn open_wal_file(&self, generation: u64) -> Result<Option<Self::WalFile>, FileError> {
		let path = self.wal_file_path(generation)?;
		if path.exists() {
			let file = WalFile::open_file(path)?;
			Ok(Some(file))
		} else {
			Ok(None)
		}
	}
}
