use std::{
	convert::Infallible,
	ffi::OsString,
	fmt,
	fs::{self, ReadDir},
	io,
	num::{NonZero, NonZeroU16, NonZeroU64},
	path::PathBuf,
};

#[cfg(feature = "io_uring")]
use io_uring::squeue::PushError;
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
	#[error("The file is not a BeeDB database file")]
	MissingMagic,

	#[error("The file was created on a platform with a different byte order and cannot be opened")]
	ByteOrderMismatch,

	#[error("The file is corrupted: {0}")]
	Corrupted(String),

	#[error("Unexpected file type {0:?}")]
	WrongFileType(FileType),

	#[error("Incompatible version of {0:?} file: {1}")]
	IncompatibleVersion(FileType, u8),

	#[error("Incompatible page version: {0}")]
	IncompatiblePageVersion(u8),

	#[error("Unexpected end of file")]
	UnexpectedEof,

	#[error("The file is corrupted; a checksum mismatch occurred")]
	ChecksumMismatch,

	#[error("Unexpected file in database folder: {}", _0.to_string_lossy())]
	UnexpectedFile(OsString),

	#[error("Too many concurrent IO operations!")]
	TooManyConcurrent,

	#[cfg(feature = "io_uring")]
	#[error("Failed to push to IO queue: {0:?}")]
	IoQueuePush(#[from] PushError),

	#[error("Concurrent read failed with code {0}")]
	ConcurrentReadFail(i32),

	#[error("Concurrent write failed with code {0}")]
	ConcurrentWriteFail(i32),

	#[error("An unexpected IO error occurred")]
	Unexpected,

	#[error(transparent)]
	Io(io::Error),
}

impl From<io::Error> for FileError {
	fn from(value: io::Error) -> Self {
		match value.kind() {
			io::ErrorKind::UnexpectedEof => Self::UnexpectedEof,
			_ => Self::Io(value),
		}
	}
}

impl From<Infallible> for FileError {
	fn from(value: Infallible) -> Self {
		match value {}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionState {
	pub first_gen: u64,
	pub last_index: WalIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PageAddress {
	pub segment_num: u32,
	pub page_num: NonZeroU16,
}

impl PageAddress {
	pub const fn new(segment_num: u32, page_num: NonZeroU16) -> Self {
		Self {
			segment_num,
			page_num,
		}
	}

	pub const fn new_unwrap(segment_num: u32, page_num: u16) -> Self {
		if page_num == 0 {
			panic!("Called PageAddress::new_unchecked with a page_num of 0");
		}
		// Safety: page_num is guaranteed not to be zero.
		let page_num = unsafe { NonZero::new_unchecked(page_num) };
		Self::new(segment_num, page_num)
	}
}

impl fmt::Display for PageAddress {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:08x}:{:04x}", self.segment_num, self.page_num)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct WalIndex {
	pub generation: u64,
	pub offset: NonZeroU64,
}

impl WalIndex {
	pub fn new(generation: u64, offset: NonZeroU64) -> Self {
		Self { generation, offset }
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

	fn segment_file_path(&self, segment_num: u32) -> Result<PathBuf, FileError> {
		self.segments_dir().map(|p| p.join(segment_num.to_string()))
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
    type IterWalFiles = std::vec::IntoIter<Result<(u64, MockWalFileApi), FileError>>;
), allow(clippy::type_complexity))]
pub(crate) trait DatabaseFolderApi {
	type SegmentFile: SegmentFileApi + Send + Sync;
	type WalFile: WalFileApi + Send + Sync;
	type IterWalFiles: Iterator<Item = Result<(u64, Self::WalFile), FileError>>;

	fn open_segment_file(&self, segment_num: u32) -> Result<Self::SegmentFile, FileError>;
	fn open_wal_file(&self, generation: u64) -> Result<Self::WalFile, FileError>;
	fn delete_wal_file(&self, generation: u64) -> Result<(), FileError>;
	fn iter_wal_files(&self) -> Result<Self::IterWalFiles, FileError>;
	fn clear_wal_files(&self) -> Result<(), FileError>;
}

impl DatabaseFolderApi for DatabaseFolder {
	type SegmentFile = SegmentFile;
	type WalFile = WalFile;
	type IterWalFiles = IterWalFiles;

	fn open_segment_file(&self, segment_num: u32) -> Result<Self::SegmentFile, FileError> {
		let path = self.segment_file_path(segment_num)?;
		if path.exists() {
			SegmentFile::open_file(path)
		} else {
			SegmentFile::create_file(path)
		}
	}

	fn open_wal_file(&self, generation: u64) -> Result<Self::WalFile, FileError> {
		let path = self.wal_file_path(generation)?;
		if path.exists() {
			WalFile::open_file(path)
		} else {
			WalFile::create_file(path)
		}
	}

	fn delete_wal_file(&self, generation: u64) -> Result<(), FileError> {
		let path = self.wal_file_path(generation)?;
		fs::remove_file(path)?;
		Ok(())
	}

	fn clear_wal_files(&self) -> Result<(), FileError> {
		fs::remove_dir_all(self.wal_dir()?)?;
		Ok(())
	}

	fn iter_wal_files(&self) -> Result<Self::IterWalFiles, FileError> {
		Ok(IterWalFiles(fs::read_dir(self.wal_dir()?)?))
	}
}

pub(crate) struct IterWalFiles(ReadDir);

impl Iterator for IterWalFiles {
	type Item = Result<(u64, WalFile), FileError>;

	fn next(&mut self) -> Option<Self::Item> {
		for entry_result in &mut self.0 {
			let entry = match entry_result {
				Ok(entry) => entry,
				Err(error) => return Some(Err(error.into())),
			};
			if entry.path().is_file() {
				let file = match WalFile::open_file(entry.path()) {
					Ok(file) => file,
					Err(error) => return Some(Err(error)),
				};
				let Ok(generation): Result<u64, _> = entry.file_name().to_string_lossy().parse()
				else {
					return Some(Err(FileError::UnexpectedFile(entry.file_name())));
				};

				return Some(Ok((generation, file)));
			}
		}

		None
	}
}

#[cfg(test)]
pub(crate) mod test_helpers {
	macro_rules! page_address {
		($segment:expr, $page:expr) => {
			$crate::files::PageAddress::new($segment, std::num::NonZeroU16::new($page).unwrap())
		};
	}
	pub(crate) use page_address;

	macro_rules! wal_index {
		($gen:expr, $offset:expr) => {
			$crate::files::WalIndex::new($gen, std::num::NonZeroU64::new($offset).unwrap())
		};
	}
	pub(crate) use wal_index;
}
