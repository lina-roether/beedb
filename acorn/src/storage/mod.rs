use std::{
	fs::{self, File},
	io,
	path::{Path, PathBuf},
};

use thiserror::Error;

use crate::{consts::DEFAULT_PAGE_SIZE, segment::Segment};

use self::{dir::StorageDir, meta::StorageMetaFile};

mod dir;
mod meta;

pub struct Storage {
	meta: StorageMetaFile<File>,
	dir: StorageDir,
	segments: Vec<Segment<File>>,
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Failed to initialize meta file: {0}")]
	Meta(#[from] meta::InitError),

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
		let mut meta_file = dir.open_meta().map_err(meta::InitError::from)?;
		StorageMetaFile::init(
			&mut meta_file,
			meta::InitParams {
				page_size: params.page_size,
			},
		)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use crate::{storage::meta::StorageMeta, utils::byte_view::ByteView};

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
}
