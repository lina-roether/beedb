use std::{
	fs::{File, OpenOptions},
	io::{Seek, SeekFrom},
	num::NonZeroU16,
	os,
	path::Path,
};

#[cfg(test)]
use mockall::automock;

use super::{generic::GenericHeader, utils::Serialized, FileError};
use crate::{consts::PAGE_SIZE, files::generic::FileType};

const FORMAT_VERSION: u8 = 1;

cfg_match! {
	cfg(not(test)) => {
		// 1 GiB for PAGE_SIZE = 16 KiB
		const SEGMENT_SIZE: usize = PAGE_SIZE << 16;
	}
	cfg(test) => {
		// Smaller segment size for testing
		const SEGMENT_SIZE: usize = PAGE_SIZE * 8;
	}
}

pub(crate) struct SegmentFile {
	file: File,
}

impl SegmentFile {
	pub fn create_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		let mut file = OpenOptions::new()
			.create(true)
			.truncate(true)
			.read(true)
			.write(true)
			.open(path)?;

		let header = GenericHeader {
			file_type: FileType::Segment,
			content_offset: PAGE_SIZE as u16,
			version: FORMAT_VERSION,
		};
		header.serialize(&mut file)?;

		file.set_len(SEGMENT_SIZE as u64)?;

		Ok(Self { file })
	}

	pub fn open_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		let mut file = OpenOptions::new().read(true).write(true).open(path)?;

		file.seek(SeekFrom::Start(0))?;
		let header = GenericHeader::deserialize(&mut file)?;

		if header.file_type != FileType::Segment {
			return Err(FileError::WrongFileType(header.file_type));
		}
		if header.version != FORMAT_VERSION {
			return Err(FileError::IncompatibleVersion(
				header.file_type,
				FORMAT_VERSION,
			));
		}
		if header.content_offset as usize != PAGE_SIZE {
			return Err(FileError::Corrupted(format!(
				"Expected content offset {PAGE_SIZE}, but found {}",
				header.content_offset
			)));
		}
		if file.metadata()?.len() != SEGMENT_SIZE as u64 {
			return Err(FileError::Corrupted(
				"Storage segment has been truncated".to_string(),
			));
		}

		Ok(Self { file })
	}

	cfg_match! {
		cfg(unix) => {
			fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), FileError> {
				os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset)?;
				Ok(())
			}

			fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), FileError> {
				os::unix::fs::FileExt::write_all_at(&self.file, buf, offset)?;
				Ok(())
			}
		}
		_ => {
			compiler_error!("Functionality not implemented on this platform!");
		}
	}

	#[inline]
	fn get_page_offset(page_num: NonZeroU16) -> u64 {
		page_num.get() as u64 * PAGE_SIZE as u64
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait SegmentFileApi {
	fn read(&self, page_num: NonZeroU16, offset: u16, buf: &mut [u8]) -> Result<(), FileError>;
	fn write(&self, page_num: NonZeroU16, offset: u16, buf: &[u8]) -> Result<(), FileError>;
}

impl SegmentFileApi for SegmentFile {
	fn read(&self, page_num: NonZeroU16, offset: u16, buf: &mut [u8]) -> Result<(), FileError> {
		assert!(offset as usize + buf.len() <= PAGE_SIZE);

		self.read_exact_at(buf, Self::get_page_offset(page_num) + offset as u64)?;
		Ok(())
	}

	fn write(&self, page_num: NonZeroU16, offset: u16, buf: &[u8]) -> Result<(), FileError> {
		assert!(offset as usize + buf.len() <= PAGE_SIZE);

		self.write_all_at(buf, Self::get_page_offset(page_num) + offset as u64)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use zerocopy::AsBytes;

	use crate::files::generic::GenericHeaderRepr;

	use super::*;

	#[test]
	fn create_segment_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();

		// when
		SegmentFile::create_file(tempdir.path().join("0")).unwrap();

		// then
		let mut expected: Vec<u8> = vec![0; SEGMENT_SIZE];
		expected[0..GenericHeader::REPR_SIZE].copy_from_slice(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Segment,
				content_offset: PAGE_SIZE as u16,
				version: FORMAT_VERSION,
			})
			.as_bytes(),
		);
		assert_eq!(fs::read(tempdir.path().join("0")).unwrap(), expected);
	}

	#[test]
	fn open_segment_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		let mut file: Vec<u8> = vec![0; SEGMENT_SIZE];
		file[0..GenericHeader::REPR_SIZE].copy_from_slice(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Segment,
				content_offset: PAGE_SIZE as u16,
				version: FORMAT_VERSION,
			})
			.as_bytes(),
		);
		fs::write(tempdir.path().join("0"), &file).unwrap();

		// then
		SegmentFile::open_file(tempdir.path().join("0")).unwrap();
	}

	#[test]
	fn write_to_page() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		let segment = SegmentFile::create_file(tempdir.path().join("0")).unwrap();

		// when
		segment
			.write(NonZeroU16::new(3).unwrap(), 2, &[1, 2, 3])
			.unwrap();

		// then
		let file = fs::read(tempdir.path().join("0")).unwrap();
		assert_eq!(
			&file[3 * PAGE_SIZE..3 * PAGE_SIZE + 8],
			&[0, 0, 1, 2, 3, 0, 0, 0]
		);
	}

	#[test]
	fn read_from_page() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		let segment = SegmentFile::create_file(tempdir.path().join("0")).unwrap();
		segment
			.write(NonZeroU16::new(5).unwrap(), 3, &[4, 5, 6])
			.unwrap();

		// when
		let mut data = [0; 3];
		segment
			.read(NonZeroU16::new(5).unwrap(), 3, &mut data)
			.unwrap();

		// then
		assert_eq!(data, [4, 5, 6]);
	}
}
