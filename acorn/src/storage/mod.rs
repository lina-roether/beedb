use std::{
	fs::{self, File},
	io,
	path::{Path, PathBuf},
};

use parking_lot::RwLock;
use thiserror::Error;

use crate::{consts::DEFAULT_PAGE_SIZE, segment::Segment};

use self::{dir::StorageDir, meta::StorageMetaFile};

mod dir;
mod meta;

pub struct Storage {
	meta: RwLock<StorageMetaFile<File>>,
	dir: StorageDir,
	segments: RwLock<Vec<Segment<File>>>,
}

#[derive(Debug, Error)]
pub enum Error {
	#[error("The folder {} doesn't exist", _0.display())]
	DoesntExist(PathBuf),

	#[error("Failed to open storage meta file: {0}")]
	FailedToOpenMeta(io::Error),

	#[error(transparent)]
	Meta(#[from] meta::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Failed to initialize meta file: {0}")]
	Meta(#[from] meta::InitError),

	#[error("Failed to create storage meta file: {0}")]
	FailedToCreateMeta(io::Error),

	#[error("The folder {} doesn't exist", _0.display())]
	DoesntExist(PathBuf),

	#[error("Failed to clear the target directory: {0}")]
	FailedToClear(io::Error),
}

pub struct InitParams {
	pub page_size: u16,
}

impl Default for InitParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

impl Storage {
	pub fn init(path: impl AsRef<Path>, params: InitParams) -> Result<(), InitError> {
		if !path.as_ref().exists() {
			return Err(InitError::DoesntExist(path.as_ref().into()));
		}
		fs::remove_dir_all(&path).map_err(InitError::FailedToClear)?;
		fs::create_dir(&path).map_err(InitError::FailedToClear)?;
		let dir = StorageDir::new(path.as_ref().into());
		let mut meta_file = dir
			.open_meta_file(true)
			.map_err(InitError::FailedToCreateMeta)?;
		StorageMetaFile::init(
			&mut meta_file,
			meta::InitParams {
				page_size: params.page_size,
			},
		)?;
		Ok(())
	}

	pub fn load(path: PathBuf) -> Result<Self, Error> {
		if !path.exists() {
			return Err(Error::DoesntExist(path));
		}
		let dir = StorageDir::new(path);
		let meta_file = dir.open_meta_file(false).map_err(Error::FailedToOpenMeta)?;
		let meta = StorageMetaFile::load(meta_file)?;

		Ok(Self {
			meta: RwLock::new(meta),
			dir,
			segments: RwLock::new(Vec::new()),
		})
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.meta.read().get().page_size()
	}
}

#[cfg(test)]
mod tests {
	use std::mem::size_of;

	use tempfile::tempdir;

	use crate::{
		storage::meta::StorageMeta,
		utils::{byte_order::ByteOrder, byte_view::ByteView},
	};

	use super::*;

	#[test]
	fn init() {
		let dir = tempdir().unwrap();

		fs::write(dir.path().join("junk"), [1, 2, 3]).unwrap();

		Storage::init(dir.path(), InitParams::default()).unwrap();

		let meta_data = fs::read(dir.path().join("storage.acnm")).unwrap();
		let meta = StorageMeta::from_bytes(&meta_data);

		assert!(!dir.path().join("junk").exists());
		assert_eq!(meta.page_size_exponent, 14);
	}

	#[test]
	fn load() {
		let dir = tempdir().unwrap();
		let mut meta_data: [u8; size_of::<StorageMeta>()] = Default::default();
		*StorageMeta::from_bytes_mut(&mut meta_data) = StorageMeta {
			magic: *b"ACNM",
			byte_order: ByteOrder::NATIVE as u8,
			format_version: 1,
			page_size_exponent: 14,
			num_clusters: 0,
		};
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		assert_eq!(storage.page_size(), 1 << 14);
	}
}
