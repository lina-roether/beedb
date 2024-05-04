use std::{
	fs::{File, OpenOptions},
	num::NonZeroU16,
	ops::{Deref, DerefMut, Range},
	path::Path,
};

use memmap2::MmapMut;

#[cfg(test)]
use mockall::automock;

use super::{generic::GenericHeader, utils::Serialized, FileError};
use crate::{consts::PAGE_SIZE, files::generic::FileType};

// 1 GiB for PAGE_SIZE = 16 KiB
const SEGMENT_SIZE: usize = PAGE_SIZE << 16;

pub(crate) struct SegmentFile<F = MmapMut>
where
	F: Deref<Target = [u8]> + DerefMut,
{
	buffer: F,
}

impl SegmentFile {
	pub fn create_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		let file = OpenOptions::new()
			.create(true)
			.truncate(true)
			.read(true)
			.write(true)
			.open(path)?;
		file.set_len(SEGMENT_SIZE as u64)?;

		Ok(Self::create(Self::map_buffer(&file)?))
	}

	pub fn open_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		let file = OpenOptions::new().read(true).write(true).open(path)?;

		Self::open(Self::map_buffer(&file)?)
	}

	fn map_buffer(file: &File) -> Result<MmapMut, FileError> {
		// Safety: memory mapping is inherently risky, but the code that accesses the
		// content should be robust enough to deal with memory corruption
		Ok(unsafe { MmapMut::map_mut(file)? })
	}
}

impl<F> SegmentFile<F>
where
	F: Deref<Target = [u8]> + DerefMut,
{
	fn create(mut buffer: F) -> Self {
		assert_eq!(buffer.len(), SEGMENT_SIZE);

		let header = GenericHeader {
			file_type: FileType::Segment,
			content_offset: PAGE_SIZE as u16,
		};
		header.write_repr_bytes(&mut buffer);
		Self { buffer }
	}

	fn open(buffer: F) -> Result<Self, FileError> {
		let header = GenericHeader::from_repr_bytes(&buffer)?;
		if header.file_type != FileType::Segment {
			return Err(FileError::WrongFileType(header.file_type));
		}
		if header.content_offset as usize != PAGE_SIZE {
			return Err(FileError::Corrupted(format!(
				"Expected content offset {PAGE_SIZE}, but found {}",
				header.content_offset
			)));
		}
		if buffer.len() != SEGMENT_SIZE {
			return Err(FileError::Corrupted(
				"Storage segment has been truncated".to_string(),
			));
		}

		Ok(Self { buffer })
	}

	#[inline]
	fn get_page_range(page_id: NonZeroU16) -> Range<usize> {
		let start = page_id.get() as usize * PAGE_SIZE;
		start..start + PAGE_SIZE
	}

	#[inline]
	fn get_page_section_range(page_id: NonZeroU16, offset: u16, len: usize) -> Range<usize> {
		let page_range = Self::get_page_range(page_id);
		let start = page_range.start + offset as usize;
		start..start + len
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait SegmentFileApi {
	fn read(&self, page_id: NonZeroU16, offset: u16, buf: &mut [u8]);
	fn write(&mut self, page_id: NonZeroU16, offset: u16, buf: &[u8]);
}

impl<F> SegmentFileApi for SegmentFile<F>
where
	F: Deref<Target = [u8]> + DerefMut,
{
	fn read(&self, page_id: NonZeroU16, offset: u16, buf: &mut [u8]) {
		assert!(offset as usize + buf.len() <= PAGE_SIZE);

		buf.copy_from_slice(&self.buffer[Self::get_page_section_range(page_id, offset, buf.len())]);
	}

	fn write(&mut self, page_id: NonZeroU16, offset: u16, buf: &[u8]) {
		assert!(offset as usize + buf.len() <= PAGE_SIZE);

		self.buffer[Self::get_page_section_range(page_id, offset, buf.len())].copy_from_slice(buf);
	}
}

#[cfg(test)]
mod tests {
	use zerocopy::AsBytes;

	use crate::files::generic::GenericHeaderRepr;

	use super::*;

	#[test]
	fn create_segment_file() {
		// given
		let mut file: Vec<u8> = vec![0; SEGMENT_SIZE];

		// when
		SegmentFile::create(&mut *file);

		// then
		let mut expected: Vec<u8> = vec![0; SEGMENT_SIZE];
		expected[0..GenericHeader::REPR_SIZE].copy_from_slice(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Segment,
				content_offset: PAGE_SIZE as u16,
			})
			.as_bytes(),
		);
		assert_eq!(file, expected);
	}

	#[test]
	fn open_segment_file() {
		// given
		let mut file: Vec<u8> = vec![0; SEGMENT_SIZE];
		file[0..GenericHeader::REPR_SIZE].copy_from_slice(
			GenericHeaderRepr::from(GenericHeader {
				file_type: FileType::Segment,
				content_offset: PAGE_SIZE as u16,
			})
			.as_bytes(),
		);

		// then
		SegmentFile::open(&mut *file).unwrap();
	}

	#[test]
	fn read_from_page() {
		// given
		let mut file: Vec<u8> = vec![0; SEGMENT_SIZE];
		let mut segment = SegmentFile::create(&mut *file);

		// when
		segment.write(NonZeroU16::new(3).unwrap(), 2, &[1, 2, 3]);

		// then
		assert_eq!(
			&file[3 * PAGE_SIZE..3 * PAGE_SIZE + 8],
			&[0, 0, 1, 2, 3, 0, 0, 0]
		);
	}

	#[test]
	fn write_to_page() {
		// given
		let mut file: Vec<u8> = vec![0; SEGMENT_SIZE];
		file[5 * PAGE_SIZE..5 * PAGE_SIZE + 10].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

		let segment = SegmentFile::create(&mut *file);

		// when
		let mut data = [0; 3];
		segment.read(NonZeroU16::new(5).unwrap(), 3, &mut data);

		// then
		assert_eq!(data, [4, 5, 6]);
	}

	#[test]
	fn create_physical_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();

		// when
		let mut segment = SegmentFile::create_file(tempdir.path().join("0")).unwrap();
		segment.write(NonZeroU16::new(2).unwrap(), 3, &[1, 2, 3]);

		// then
		assert!(tempdir.path().join("0").exists());
		let mut data = [0; 3];
		segment.read(NonZeroU16::new(2).unwrap(), 3, &mut data);
		assert_eq!(data, [1, 2, 3]);
	}

	#[test]
	fn open_physical_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		SegmentFile::create_file(tempdir.path().join("0")).unwrap();

		// when
		let mut segment = SegmentFile::open_file(tempdir.path().join("0")).unwrap();
		segment.write(NonZeroU16::new(2).unwrap(), 3, &[1, 2, 3]);

		// then
		let mut data = [0; 3];
		segment.read(NonZeroU16::new(2).unwrap(), 3, &mut data);
		assert_eq!(data, [1, 2, 3]);
	}
}
