use std::{
	fs::File,
	io,
	path::{Path, PathBuf},
	usize,
};

use parking_lot::RwLock;
use static_assertions::assert_impl_all;
use thiserror::Error;

use crate::{index::PageId, utils::array_map::ArrayMap};

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
	CreateMeta(io::Error),

	#[error("Failed to initialize storage meta: {0}")]
	InitMeta(#[from] meta::InitError),
}

#[derive(Debug, Error)]
pub enum LoadError {
	#[error("The directory {} doesn't exist", _0.display())]
	NoSuchDir(PathBuf),

	#[error("{} is not a directory", _0.display())]
	NotADirectory(PathBuf),

	#[error("Failed to open the storage meta file: {0}")]
	OpenMeta(io::Error),

	#[error("Failed to load the storage metadata: {0}")]
	LoadMeta(#[from] meta::LoadError),

	#[error(transparent)]
	Err(#[from] Error),
}

#[derive(Debug, Error)]
pub enum Error {
	#[error("Failed to update storage metadata: {0}")]
	MetaWrite(io::Error),

	#[error("Failed to open file for segment {0}: {1}")]
	OpenSegment(u32, io::Error),

	#[error("Failed to load segment {0}: {1}")]
	LoadSegment(u32, segment::LoadError),

	#[error("Failed to create a new segment file: {0}")]
	CreateSegment(io::Error),

	#[error("Failed to initialize new segment: {0}")]
	InitSegment(segment::InitError),

	#[error("Failed to read page {0}: {1}")]
	PageRead(PageId, io::Error),

	#[error("Failed to write to page {0}: {1}")]
	PageWrite(PageId, io::Error),
}

pub struct DiskStorage {
	page_size: u16,
	dir: StorageDir,
	state: RwLock<State>,
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
		let mut meta_file = dir.open_meta_file(true).map_err(InitError::CreateMeta)?;
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
		let meta_file = dir.open_meta_file(false).map_err(LoadError::OpenMeta)?;
		let meta = StorageMetaFile::load(meta_file).map_err(LoadError::LoadMeta)?;
		let disk_storage = DiskStorage {
			page_size: meta.page_size(),
			dir,
			state: RwLock::new(State {
				meta,
				segment_files: ArrayMap::new(),
			}),
		};
		disk_storage.load_all_segment_files()?;
		Ok(disk_storage)
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.page_size
	}

	pub fn segments(&self) -> Box<[u32]> {
		self.state.read().iter_loaded_segments().collect()
	}

	pub fn read_page(&self, buf: &mut [u8], page_id: PageId) -> Result<(), Error> {
		self.ensure_segment_exists(page_id.segment_num)?;

		let state = self.state.read();
		let segment_file = state
			.get_loaded_segment(page_id.segment_num)
			.expect("Segment should have been opened for reading");
		segment_file
			.read_page(buf, page_id.page_num)
			.map_err(|err| Error::PageRead(page_id, err))?;
		Ok(())
	}

	pub fn write_page(&self, buf: &[u8], page_id: PageId) -> Result<(), Error> {
		self.ensure_segment_exists(page_id.segment_num)?;

		let state = self.state.read();
		let segment_file = state
			.get_loaded_segment(page_id.segment_num)
			.expect("Segment should have been opened for writing");
		segment_file
			.write_page(buf, page_id.page_num)
			.map_err(|err| Error::PageWrite(page_id, err))?;
		Ok(())
	}

	fn ensure_segment_exists(&self, segment_num: u32) -> Result<(), Error> {
		if !self.dir.segment_file_exists(segment_num) {
			self.create_segment(segment_num)
		} else {
			Ok(())
		}
	}

	fn create_segment(&self, segment_num: u32) -> Result<(), Error> {
		let mut state = self.state.write();
		let mut file = self
			.dir
			.open_segment_file(segment_num, true)
			.map_err(Error::CreateSegment)?;
		SegmentFile::init(
			&mut file,
			segment::InitParams {
				page_size: self.page_size(),
			},
		)
		.map_err(Error::InitSegment)?;
		let segment = self.open_segment(segment_num)?;
		state.insert_loaded_segment(segment_num, segment)?;
		Ok(())
	}

	fn load_all_segment_files(&self) -> Result<(), Error> {
		let mut state = self.state.write();
		state.clear_segments();

		for segment_num in 0..state.meta.segment_num_limit {
			if !self.dir.segment_file_exists(segment_num) {
				continue;
			}

			let segment = self.open_segment(segment_num)?;
			state.insert_loaded_segment(segment_num, segment)?;
		}

		Ok(())
	}

	fn open_segment(&self, segment_num: u32) -> Result<SegmentFile<File>, Error> {
		let file = self
			.dir
			.open_segment_file(segment_num, false)
			.map_err(|err| Error::OpenSegment(segment_num, err))?;
		SegmentFile::load(
			file,
			segment::LoadParams {
				page_size: self.page_size,
			},
		)
		.map_err(|err| Error::LoadSegment(segment_num, err))
	}
}

struct State {
	meta: StorageMetaFile<File>,
	segment_files: ArrayMap<SegmentFile<File>>,
}

impl State {
	fn iter_loaded_segments(&self) -> impl Iterator<Item = u32> + '_ {
		self.segment_files.iter().map(|(k, _)| k as u32)
	}

	fn flush_meta(&mut self) -> Result<(), Error> {
		self.meta.flush().map_err(Error::MetaWrite)
	}

	fn clear_segments(&mut self) {
		self.segment_files.clear();
	}

	fn get_loaded_segment(&self, segment_num: u32) -> Option<&SegmentFile<File>> {
		self.segment_files.get(segment_num as usize)
	}

	fn insert_loaded_segment(
		&mut self,
		segment_num: u32,
		segment: SegmentFile<File>,
	) -> Result<(), Error> {
		if segment_num >= self.meta.segment_num_limit {
			self.meta.segment_num_limit = segment_num + 1;
			self.flush_meta()?;
		}
		self.segment_files.insert(segment_num as usize, segment);
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

	use byte_view::{ByteView, ViewBuf};
	use tempfile::tempdir;

	use crate::{
		consts::SEGMENT_MAGIC,
		disk::meta::StorageMeta,
		utils::{byte_order::ByteOrder, units::KiB},
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
		assert_eq!(meta.segment_num_limit, 0);
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
		let mut meta: ViewBuf<StorageMeta> = ViewBuf::new();
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.segment_num_limit = 1;
		fs::write(dir.path().join("storage.acnm"), meta.as_bytes()).unwrap();

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
	fn read_write_page() {
		let dir = tempdir().unwrap();
		let mut meta: ViewBuf<StorageMeta> = ViewBuf::new();
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.segment_num_limit = 1;
		fs::write(dir.path().join("storage.acnm"), meta.as_bytes()).unwrap();

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
	fn read_from_nonexistent_segment() {
		let dir = tempdir().unwrap();
		let mut meta: ViewBuf<StorageMeta> = ViewBuf::new();
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.segment_num_limit = 0;
		fs::write(dir.path().join("storage.acnm"), meta.as_bytes()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let page_id = PageId::new(0, 1);

		let mut dest_buf: Box<[u8]> = iter::repeat(0xaa)
			.take(storage.page_size().into())
			.collect();
		storage.read_page(&mut dest_buf, page_id).unwrap();
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn segment_is_initialized_on_read() {
		let dir = tempdir().unwrap();
		let mut meta: ViewBuf<StorageMeta> = ViewBuf::new();
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.segment_num_limit = 0;
		fs::write(dir.path().join("storage.acnm"), meta.as_bytes()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let page_id = PageId::new(0, 0);

		let mut dest_buf: Box<[u8]> = iter::repeat(0xaa)
			.take(storage.page_size().into())
			.collect();
		storage.read_page(&mut dest_buf, page_id).unwrap();

		assert!(dest_buf.starts_with(&SEGMENT_MAGIC));
	}

	#[test]
	#[cfg_attr(miri, ignore)]
	fn write_to_nonexistent_segment() {
		let dir = tempdir().unwrap();
		let mut meta: ViewBuf<StorageMeta> = ViewBuf::new();
		meta.magic = *b"ACNM";
		meta.format_version = 1;
		meta.byte_order = ByteOrder::NATIVE as u8;
		meta.page_size_exponent = 14;
		meta.segment_num_limit = 0;
		fs::write(dir.path().join("storage.acnm"), meta.as_bytes()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let page_id = PageId::new(0, 1);

		let source_buf: Box<[u8]> = iter::repeat(25).take(storage.page_size().into()).collect();
		storage.write_page(&source_buf, page_id).unwrap();

		let mut dest_buf: Box<[u8]> = iter::repeat(0).take(storage.page_size().into()).collect();
		storage.read_page(&mut dest_buf, page_id).unwrap();

		assert_eq!(dest_buf, source_buf);
		assert!(dir.path().join("0.acns").exists())
	}
}
