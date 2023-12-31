use std::{
	fs::File,
	io, mem,
	path::{Path, PathBuf},
	usize,
};

use parking_lot::RwLock;
use static_assertions::assert_impl_all;
use thiserror::Error;

use crate::index::PageId;

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
	#[error("Failed to update storage metadata: {0}")]
	MetaWriteFailed(io::Error),

	#[error("Failed to open file for segment {0}: {1}")]
	OpenSegmentFailed(u32, io::Error),

	#[error("Failed to load segment {0}: {1}")]
	LoadSegmentFailed(u32, segment::LoadError),

	#[error("Failed to create a new segment file: {0}")]
	CreateSegmentFailed(io::Error),

	#[error("Failed to initialize new segment: {0}")]
	InitSegmentFailed(segment::InitError),

	#[error("Segment {0} doesn't exist")]
	NoSuchSegment(u32),

	#[error("Failed to read page {0}: {1}")]
	PageReadFailed(PageId, io::Error),

	#[error("Failed to write to page {0}: {1}")]
	PageWriteFailed(PageId, io::Error),
}

pub struct DiskStorage {
	page_size: u16,
	meta: RwLock<StorageMetaFile<File>>,
	dir: StorageDir,
	segment_files: RwLock<Vec<SegmentFile<File>>>,
}

assert_impl_all!(DiskStorage: Send, Sync);

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
			page_size: meta.get().page_size(),
			meta: RwLock::new(meta),
			dir,
			segment_files: RwLock::new(Vec::new()),
		};
		disk_storage.sync_loaded_segment_files()?;
		Ok(disk_storage)
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.page_size
	}

	#[inline]
	pub fn num_segments(&self) -> u32 {
		self.meta.read().get().num_segments
	}

	pub fn read_page(&self, buf: &mut [u8], page_id: PageId) -> Result<(), Error> {
		let segment_files = self.segment_files.read();
		let Some(segment_file) = segment_files.get(page_id.segment_num as usize) else {
			return Err(Error::NoSuchSegment(page_id.segment_num));
		};
		segment_file
			.read_page(buf, page_id.page_num)
			.map_err(|err| Error::PageReadFailed(page_id, err))?;
		Ok(())
	}

	pub fn write_page(&self, buf: &[u8], page_id: PageId) -> Result<(), Error> {
		let segment_files = self.segment_files.read();
		let Some(segment_file) = segment_files.get(page_id.segment_num as usize) else {
			return Err(Error::NoSuchSegment(page_id.segment_num));
		};
		segment_file
			.write_page(buf, page_id.page_num)
			.map_err(|err| Error::PageWriteFailed(page_id, err))?;
		Ok(())
	}

	pub fn new_segment(&self) -> Result<u32, Error> {
		let mut meta_file = self.meta.write();
		let meta = meta_file.get_mut();
		let segment_num = meta.num_segments;
		let mut file = self
			.dir
			.open_segment_file(segment_num, true)
			.map_err(Error::CreateSegmentFailed)?;
		SegmentFile::init(
			&mut file,
			segment::InitParams {
				page_size: self.page_size(),
			},
		)
		.map_err(Error::InitSegmentFailed)?;
		meta.num_segments += 1;
		meta_file.flush().map_err(Error::MetaWriteFailed)?;
		mem::drop(meta_file);
		self.sync_loaded_segment_files()?;
		Ok(segment_num)
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
		iter,
	};

	use tempfile::tempdir;

	use crate::{
		disk::meta::StorageMeta,
		utils::{
			byte_order::ByteOrder,
			byte_view::{AlignedBytes, ByteView},
			units::KiB,
		},
	};

	use super::*;

	#[test]
	#[cfg_attr(miri, ignore)]
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
	#[cfg_attr(miri, ignore)]
	fn load() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBytes<12> = Default::default();
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

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		assert_eq!(storage.page_size(), 16 * KiB as u16);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn try_load_with_missing_segment_file() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBytes<12> = Default::default();
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

	#[test]
	#[cfg_attr(miri, ignore)]
	fn read_write_page() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBytes<12> = Default::default();
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

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let page_id = PageId::new(0, 1);

		let source_buf: Box<[u8]> = iter::repeat(25).take(storage.page_size().into()).collect();
		storage.write_page(&source_buf, page_id).unwrap();

		let mut dest_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();
		storage.read_page(&mut dest_buf, page_id).unwrap();

		assert_eq!(dest_buf, source_buf);
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn create_new_segment() {
		let dir = tempdir().unwrap();
		let mut meta_data: AlignedBytes<12> = Default::default();
		let meta = StorageMeta::from_bytes_mut(meta_data.as_mut());
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.num_segments = 0;
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		assert_eq!(storage.new_segment().unwrap(), 0);
		assert_eq!(storage.new_segment().unwrap(), 1);

		// Should be able to load created segments
		SegmentFile::load(
			File::open(dir.path().join("0.acns")).unwrap(),
			segment::LoadParams {
				page_size: storage.page_size(),
			},
		)
		.unwrap();

		SegmentFile::load(
			File::open(dir.path().join("1.acns")).unwrap(),
			segment::LoadParams {
				page_size: storage.page_size(),
			},
		)
		.unwrap();
	}
}
