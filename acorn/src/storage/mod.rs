use std::{
	fs::{self, File},
	io, mem,
	path::{Path, PathBuf},
	usize,
};

use parking_lot::RwLock;
use thiserror::Error;

use crate::{
	consts::DEFAULT_PAGE_SIZE,
	index::PageId,
	segment::{self, Segment},
};

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

	#[error("Failed to open file for segment {0}: {1}")]
	FailedToOpenSegment(u32, io::Error),

	#[error("Error accessing segment {0}: {1}")]
	Segment(u32, segment::Error),

	#[error("Segment {0} doesn't exist")]
	SegmentDoesntExist(u32),

	#[error("Failed to read page {0}: {1}")]
	ReadFailed(PageId, segment::Error),

	#[error("Failed to write to page {0}: {1}")]
	WriteFailed(PageId, segment::Error),

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

		let storage = Self {
			meta: RwLock::new(meta),
			dir,
			segments: RwLock::new(Vec::new()),
		};
		let meta = storage.meta.read();
		storage.load_missing_segments(&meta)?;
		mem::drop(meta);

		Ok(storage)
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.meta.read().get().page_size()
	}

	pub fn read_page(&self, buf: &mut [u8], id: PageId) -> Result<(), Error> {
		let segments = self.segments.read();
		let segment = self.get_segment(&segments, id.segment_num)?;
		segment
			.read_page(buf, id.page_num)
			.map_err(|err| Error::ReadFailed(id, err))?;
		Ok(())
	}

	pub fn write_page(&self, buf: &[u8], id: PageId) -> Result<(), Error> {
		let segments = self.segments.read();
		let segment = self.get_segment(&segments, id.segment_num)?;
		segment
			.write_page(buf, id.page_num)
			.map_err(|err| Error::WriteFailed(id, err))?;
		Ok(())
	}

	fn get_segment<'a>(
		&self,
		segments: &'a [Segment<File>],
		segment_num: u32,
	) -> Result<&'a Segment<File>, Error> {
		segments
			.get(segment_num as usize)
			.ok_or(Error::SegmentDoesntExist(segment_num))
	}

	fn load_missing_segments(&self, meta: &StorageMetaFile<File>) -> Result<(), Error> {
		let mut segments = self.segments.write();
		for segment_num in (segments.len() as u32)..meta.get().num_segments {
			let file = self
				.dir
				.open_segment_file(segment_num, false)
				.map_err(|err| Error::FailedToOpenSegment(segment_num, err))?;
			let segment = Segment::load(file).map_err(|err| Error::Segment(segment_num, err))?;
			segments.push(segment);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{fs::OpenOptions, iter, mem::size_of};

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
			num_segments: 0,
		};
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let storage = Storage::load(dir.path().into()).unwrap();
		assert_eq!(storage.page_size(), 1 << 14);
	}

	#[test]
	fn read_page() {
		let dir = tempdir().unwrap();
		let mut meta_data: [u8; size_of::<StorageMeta>()] = Default::default();
		*StorageMeta::from_bytes_mut(&mut meta_data) = StorageMeta {
			magic: *b"ACNM",
			byte_order: ByteOrder::NATIVE as u8,
			format_version: 1,
			page_size_exponent: 14,
			num_segments: 1,
		};
		fs::write(dir.path().join("storage.acnm"), meta_data).unwrap();

		let buf: Box<[u8]> = iter::repeat(69).take(1 << 14).collect();

		let mut segment_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(dir.path().join("0.acns"))
			.unwrap();
		Segment::init(&mut segment_file, segment::InitParams::default()).unwrap();
		let segment = Segment::load(segment_file).unwrap();
		let page_num = segment.allocate_page().unwrap();
		segment.write_page(&buf, page_num).unwrap();
		mem::drop(segment);

		let storage = Storage::load(dir.path().into()).unwrap();
		let page_id = PageId::new(0, page_num);

		let mut res_buf: Box<[u8]> = iter::repeat(0).take(1 << 14).collect();
		storage.read_page(&mut res_buf, page_id).unwrap();

		assert_eq!(res_buf, buf);
	}
}
