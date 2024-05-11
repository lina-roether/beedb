use std::{
	fs::{File, OpenOptions},
	io::{Seek, SeekFrom},
	num::{NonZeroU16, NonZeroU64},
	os,
	path::Path,
};

#[cfg(test)]
use mockall::automock;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::{generic::GenericHeader, utils::Serialized, FileError};
use crate::{
	consts::PAGE_SIZE,
	files::{generic::FileType, utils::CRC16},
	storage::WalIndex,
};

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

#[derive(Debug, Clone, FromZeroes, FromBytes, AsBytes)]
#[repr(C, packed)]
struct PageHeaderRepr {
	wal_generation: u64,
	wal_offset: u64,
	crc: u16,
	format_version: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PageHeader {
	wal_index: WalIndex,
	crc: u16,
}

const PAGE_FORMAT_VERSION: u8 = 1;

impl From<PageHeader> for PageHeaderRepr {
	fn from(value: PageHeader) -> Self {
		Self {
			wal_generation: value.wal_index.generation,
			wal_offset: value.wal_index.offset.get(),
			crc: value.crc,
			format_version: PAGE_FORMAT_VERSION,
		}
	}
}

impl TryFrom<PageHeaderRepr> for PageHeader {
	type Error = FileError;

	fn try_from(value: PageHeaderRepr) -> Result<Self, Self::Error> {
		if value.format_version != PAGE_FORMAT_VERSION {
			return Err(FileError::IncompatiblePageVersion(value.format_version));
		}
		let Some(wal_offset) = NonZeroU64::new(value.wal_offset) else {
			return Err(FileError::Corrupted(
				"Found invalid WAL offset '0'".to_string(),
			));
		};
		Ok(Self {
			wal_index: WalIndex::new(value.wal_generation, wal_offset),
			crc: value.crc,
		})
	}
}

impl Serialized for PageHeader {
	type Repr = PageHeaderRepr;
}

pub(crate) const PAGE_BODY_SIZE: usize = PAGE_SIZE - PageHeader::REPR_SIZE;

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
			compile_error!("Functionality not implemented on this platform!");
		}
	}

	#[inline]
	fn get_page_offset(page_num: NonZeroU16) -> u64 {
		page_num.get() as u64 * PAGE_SIZE as u64
	}
}

#[cfg_attr(test, automock)]
pub(crate) trait SegmentFileApi {
	fn read(&self, page_num: NonZeroU16, buf: &mut [u8]) -> Result<WalIndex, FileError>;
	fn write(&self, page_num: NonZeroU16, buf: &[u8], wal_index: WalIndex)
		-> Result<(), FileError>;
}

impl SegmentFileApi for SegmentFile {
	fn read(&self, page_num: NonZeroU16, buf: &mut [u8]) -> Result<WalIndex, FileError> {
		debug_assert_eq!(buf.len(), PAGE_BODY_SIZE);

		let mut page_buf = [0; PAGE_SIZE];
		self.read_exact_at(&mut page_buf, Self::get_page_offset(page_num))?;
		let header = PageHeader::from_repr_bytes(&page_buf[0..PageHeader::REPR_SIZE])?;
		let body = &page_buf[PageHeader::REPR_SIZE..];

		let crc = CRC16.checksum(body);
		if header.crc != crc {
			return Err(FileError::ChecksumMismatch);
		}

		buf.copy_from_slice(body);

		Ok(header.wal_index)
	}

	fn write(
		&self,
		page_num: NonZeroU16,
		buf: &[u8],
		wal_index: WalIndex,
	) -> Result<(), FileError> {
		debug_assert_eq!(buf.len(), PAGE_BODY_SIZE);

		let crc = CRC16.checksum(buf);
		let header = PageHeader { wal_index, crc };

		let mut page_buf = [0; PAGE_SIZE];
		page_buf[0..PageHeader::REPR_SIZE].copy_from_slice(header.into_repr().as_bytes());
		page_buf[PageHeader::REPR_SIZE..].copy_from_slice(buf);

		self.write_all_at(&page_buf, Self::get_page_offset(page_num))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use pretty_assertions::assert_buf_eq;
	use zerocopy::AsBytes;

	use crate::{
		files::generic::GenericHeaderRepr, storage::test_helpers::wal_index,
		utils::macros::non_zero,
	};

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
			.write(non_zero!(3), &[3; PAGE_BODY_SIZE], wal_index!(69, 420))
			.unwrap();

		// then
		let file = fs::read(tempdir.path().join("0")).unwrap();
		assert_buf_eq!(
			&file[3 * PAGE_SIZE..4 * PAGE_SIZE],
			[
				PageHeaderRepr {
					wal_generation: 69,
					wal_offset: 420,
					crc: 0x9c41,
					format_version: 1
				}
				.as_bytes(),
				&[3; PAGE_BODY_SIZE]
			]
			.concat()
		);
	}

	#[test]
	fn read_from_page() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		let segment = SegmentFile::create_file(tempdir.path().join("0")).unwrap();
		segment
			.write(non_zero!(5), &[25; PAGE_BODY_SIZE], wal_index!(69, 420))
			.unwrap();

		// when
		let mut data = [0; PAGE_BODY_SIZE];
		let wal_index = segment.read(non_zero!(5), &mut data).unwrap();

		// then
		assert_eq!(wal_index, wal_index!(69, 420));
		assert_eq!(data, [25; PAGE_BODY_SIZE]);
	}
}
