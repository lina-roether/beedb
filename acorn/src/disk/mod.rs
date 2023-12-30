use std::{
	fs::File,
	io,
	path::{Path, PathBuf},
};

use parking_lot::RwLock;
use thiserror::Error;

use self::{dir::StorageDir, meta::StorageMetaFile, segment::SegmentFile};

mod dir;
mod lock;
mod meta;
mod segment;

pub use meta::InitParams;

#[derive(Debug, Error)]
pub enum InitError {
	#[error("The directory {} doesn't exist", _0.display())]
	NoSuchDir(PathBuf),

	#[error("{} is not a directory", _0.display())]
	NotADirectory(PathBuf),

	#[error("The directory {} is not empty", _0.display())]
	NotEmpty(PathBuf),

	#[error("Couldn't create the storage meta file: {0}")]
	CreateMetaFailed(io::Error),

	#[error("Failed to initialize storage meta: {0}")]
	InitMetaFailed(#[from] meta::InitError),
}

#[derive(Debug, Error)]
pub enum LoadError {
	#[error("The directory {} doesn't exist", _0.display())]
	NoSuchDir(PathBuf),

	#[error("{} is not a directory", _0.display())]
	NotADirectory(PathBuf),

	#[error("Failed to open the storage meta file: {0}")]
	OpenMetaFailed(io::Error),

	#[error("Failed to load the storage metadata: {0}")]
	LoadMetaFailed(#[from] meta::LoadError),

	#[error(transparent)]
	Err(#[from] Error),
}

#[derive(Debug, Error)]
pub enum Error {
	#[error("Failed to open file for segment {0}: {1}")]
	OpenSegmentFailed(u32, io::Error),

	#[error("Failed to load segment {0}: {1}")]
	LoadSegmentFailed(u32, segment::LoadError),
}

pub struct DiskStorage {
	meta: RwLock<StorageMetaFile<File>>,
	dir: StorageDir,
	segment_files: RwLock<Vec<SegmentFile<File>>>,
}

impl DiskStorage {
	pub fn init(path: impl AsRef<Path>, params: InitParams) -> Result<(), InitError> {
		if !path.as_ref().exists() {
			return Err(InitError::NoSuchDir(path.as_ref().into()));
		}
		if !path.as_ref().is_dir() {
			return Err(InitError::NotADirectory(path.as_ref().into()));
		}
		if path.as_ref().read_dir().unwrap().count() != 0 {
			return Err(InitError::NotEmpty(path.as_ref().into()));
		}
		let dir = StorageDir::new(path.as_ref().into());
		let mut meta_file = dir
			.open_meta_file(true)
			.map_err(InitError::CreateMetaFailed)?;
		StorageMetaFile::init(&mut meta_file, params)?;
		Ok(())
	}

	pub fn load(path: PathBuf) -> Result<Self, LoadError> {
		if !path.exists() {
			return Err(LoadError::NoSuchDir(path));
		}
		if !path.is_dir() {
			return Err(LoadError::NotADirectory(path));
		}
		let dir = StorageDir::new(path);
		let meta_file = dir
			.open_meta_file(false)
			.map_err(LoadError::OpenMetaFailed)?;
		let meta = StorageMetaFile::load(meta_file).map_err(LoadError::LoadMetaFailed)?;
		let disk_storage = DiskStorage {
			meta: RwLock::new(meta),
			dir,
			segment_files: RwLock::new(Vec::new()),
		};
		disk_storage.sync_loaded_segment_files()?;
		Ok(disk_storage)
	}

	fn sync_loaded_segment_files(&self) -> Result<(), Error> {
		let meta_file = self.meta.read();
		let meta = meta_file.get();
		let mut segment_files = self.segment_files.write();

		if segment_files.len() >= meta.num_segments as usize {
			segment_files.truncate(meta.num_segments as usize);
			return Ok(());
		}

		for segment_num in (segment_files.len() as u32)..meta.num_segments {
			let file = self
				.dir
				.open_segment_file(segment_num, false)
				.map_err(|err| Error::OpenSegmentFailed(segment_num, err))?;
			let segment = SegmentFile::load(
				file,
				segment::LoadParams {
					page_size: meta.page_size(),
				},
			)
			.map_err(|err| Error::LoadSegmentFailed(segment_num, err))?;
			segment_files.push(segment);
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{
		assert_matches::assert_matches,
		fs::{self, OpenOptions},
	};

	use tempfile::tempdir;

	use crate::{
		disk::meta::StorageMeta,
		utils::{
			byte_order::ByteOrder,
			byte_view::{AlignedBuffer, ByteView},
			units::KiB,
		},
	};

	use super::*;

	#[test]
	fn init_dir() {
		let dir = tempdir().unwrap();

		DiskStorage::init(
			dir.path(),
			InitParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let meta_bytes = fs::read(dir.path().join("storage.acnm")).unwrap();
		let meta = StorageMeta::from_bytes(&meta_bytes);
		assert_eq!(meta.num_segments, 0);
		assert_eq!(meta.page_size_exponent, 14);
		assert_eq!(meta.magic, *b"ACNM");
		assert_eq!(meta.format_version, 1);
		assert_eq!(meta.byte_order, ByteOrder::NATIVE as u8);
	}

	#[test]
	fn try_init_non_empty_dir() {
		let dir = tempdir().unwrap();
		fs::write(dir.path().join("junk"), [0x69]).unwrap();

		let result = DiskStorage::init(
			dir.path(),
			InitParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => assert_matches!(err, InitError::NotEmpty(path) if path == dir.path()),
		}
	}

	#[test]
	fn load() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBuffer<12> = Default::default();
		let meta = StorageMeta::from_bytes_mut(meta_data.as_mut());
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.num_segments = 1;
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let mut segment_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(dir.path().join("0.acns"))
			.unwrap();
		SegmentFile::init(
			&mut segment_file,
			segment::InitParams { page_size: 1 << 14 },
		)
		.unwrap();

		DiskStorage::load(dir.path().into()).unwrap();
	}

	#[test]
	fn try_load_with_missing_segment_file() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBuffer<12> = Default::default();
		let meta = StorageMeta::from_bytes_mut(meta_data.as_mut());
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.num_segments = 1;
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let result = DiskStorage::load(dir.path().into());

		match result {
			Ok(..) => panic!("Should not succeed"),
			Err(err) => {
				assert_matches!(err, LoadError::Err(Error::OpenSegmentFailed(segment, ..)) if segment == 0)
			}
		}
	}
}
