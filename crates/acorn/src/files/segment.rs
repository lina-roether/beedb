use std::{
	fs::{File, OpenOptions},
	io::{Seek, SeekFrom},
	num::{NonZeroU16, NonZeroU64},
	os,
	path::Path,
};

#[cfg(test)]
use mockall::automock;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes};

use super::{
	generic::{GenericHeader, GenericHeaderRepr},
	FileError, WalIndex,
};
use crate::{
	consts::PAGE_SIZE,
	files::{generic::FileType, utils::CRC16},
	repr::{IoRepr, Repr},
};

const FORMAT_VERSION_UNINIT: u8 = 0;
const FORMAT_VERSION: u8 = 1;

// 2 GiB when PAGE_SIZE = 32 KiB
const SEGMENT_SIZE: usize = PAGE_SIZE << 16;

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitPageHeader {
	wal_index: WalIndex,
	crc: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PageHeader {
	Uninit,
	Init(InitPageHeader),
}

#[derive(Debug, Clone, Immutable, FromBytes, IntoBytes)]
#[repr(C, packed)]
struct PageHeaderRepr {
	wal_generation: u64,
	wal_offset: u64,
	crc: u16,
	format_version: u8,
}
impl Repr<PageHeader> for PageHeaderRepr {
	type Error = FileError;
}

const PAGE_FORMAT_VERSION: u8 = 1;

impl From<PageHeader> for PageHeaderRepr {
	fn from(value: PageHeader) -> Self {
		match value {
			PageHeader::Uninit => Self::new_zeroed(),
			PageHeader::Init(header) => Self {
				wal_generation: header.wal_index.generation,
				wal_offset: header.wal_index.offset.get(),
				crc: header.crc,
				format_version: PAGE_FORMAT_VERSION,
			},
		}
	}
}

impl TryFrom<PageHeaderRepr> for PageHeader {
	type Error = FileError;

	fn try_from(value: PageHeaderRepr) -> Result<Self, Self::Error> {
		if value.format_version == FORMAT_VERSION_UNINIT {
			return Ok(Self::Uninit);
		}

		if value.format_version != PAGE_FORMAT_VERSION {
			return Err(FileError::IncompatiblePageVersion(value.format_version));
		}
		let Some(wal_offset) = NonZeroU64::new(value.wal_offset) else {
			return Err(FileError::Corrupted(
				"Found invalid WAL offset '0'".to_string(),
			));
		};
		Ok(Self::Init(InitPageHeader {
			wal_index: WalIndex::new(value.wal_generation, wal_offset),
			crc: value.crc,
		}))
	}
}

pub(crate) const PAGE_BODY_SIZE: usize = PAGE_SIZE - PageHeaderRepr::SIZE;

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
			content_offset: u16::try_from(PAGE_SIZE).unwrap(),
			version: FORMAT_VERSION,
		};
		GenericHeaderRepr::serialize(header, &mut file)?;

		file.set_len(SEGMENT_SIZE as u64)?;

		Ok(Self { file })
	}

	pub fn open_file(path: impl AsRef<Path>) -> Result<Self, FileError> {
		let mut file = OpenOptions::new().read(true).write(true).open(path)?;

		file.seek(SeekFrom::Start(0))?;
		let header = GenericHeaderRepr::deserialize(&mut file)?;

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

	#[cfg(unix)]
	fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), FileError> {
		os::unix::fs::FileExt::read_exact_at(&self.file, buf, offset)?;
		Ok(())
	}

	#[cfg(unix)]
	fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), FileError> {
		os::unix::fs::FileExt::write_all_at(&self.file, buf, offset)?;
		Ok(())
	}

	#[cfg(not(unix))]
	compile_error!("Functionality not implemented on this platform!");

	#[inline]
	fn get_page_offset(page_num: NonZeroU16) -> u64 {
		page_num.get() as u64 * PAGE_SIZE as u64
	}
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SegmentReadOp<'a> {
	pub page_num: NonZeroU16,
	pub buf: &'a mut [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentWriteOp<'a> {
	pub page_num: NonZeroU16,
	pub wal_index: WalIndex,
	pub buf: &'a [u8],
}

#[cfg_attr(test, automock)]
pub(crate) trait SegmentFileApi {
	fn read<'a>(&self, op: SegmentReadOp<'a>) -> Result<Option<WalIndex>, FileError>;
	fn write<'a>(&self, op: SegmentWriteOp<'a>) -> Result<(), FileError>;
}

impl SegmentFileApi for SegmentFile {
	fn read(&self, op: SegmentReadOp) -> Result<Option<WalIndex>, FileError> {
		debug_assert_eq!(op.buf.len(), PAGE_BODY_SIZE);

		let mut page_buf = [0; PAGE_SIZE];
		self.read_exact_at(&mut page_buf, Self::get_page_offset(op.page_num))?;
		let header = PageHeaderRepr::from_bytes(&page_buf[0..PageHeaderRepr::SIZE])?;
		let PageHeader::Init(header) = header else {
			op.buf.fill(0);
			return Ok(None);
		};

		let body = &page_buf[PageHeaderRepr::SIZE..];

		let crc = CRC16.checksum(body);
		if header.crc != crc {
			return Err(FileError::ChecksumMismatch);
		}

		op.buf.copy_from_slice(body);

		Ok(Some(header.wal_index))
	}

	fn write(&self, op: SegmentWriteOp) -> Result<(), FileError> {
		debug_assert_eq!(op.buf.len(), PAGE_BODY_SIZE);

		let crc = CRC16.checksum(op.buf);
		let header = PageHeader::Init(InitPageHeader {
			wal_index: op.wal_index,
			crc,
		});

		let mut page_buf = [0; PAGE_SIZE];
		page_buf[0..PageHeaderRepr::SIZE].copy_from_slice(PageHeaderRepr::from(header).as_bytes());
		page_buf[PageHeaderRepr::SIZE..].copy_from_slice(op.buf);

		self.write_all_at(&page_buf, Self::get_page_offset(op.page_num))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::io::{Read, Write};

	use pretty_assertions::assert_buf_eq;

	use crate::{
		files::{generic::GenericHeaderRepr, test_helpers::wal_index},
		utils::test_helpers::non_zero,
	};

	use super::*;

	#[test]
	fn create_segment_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();

		// when
		SegmentFile::create_file(tempdir.path().join("0")).unwrap();

		// then
		let expected: Vec<u8> = GenericHeaderRepr::from(GenericHeader {
			file_type: FileType::Segment,
			content_offset: PAGE_SIZE as u16,
			version: FORMAT_VERSION,
		})
		.as_bytes()
		.to_vec();

		let mut file = File::open(tempdir.path().join("0")).unwrap();
		let received: &mut [u8] = &mut [0; GenericHeaderRepr::SIZE];
		file.read_exact(received).unwrap();

		assert_buf_eq!(received, expected);
	}

	#[test]
	fn open_segment_file() {
		// given
		let tempdir = tempfile::tempdir().unwrap();
		let file_start: Vec<u8> = GenericHeaderRepr::from(GenericHeader {
			file_type: FileType::Segment,
			content_offset: PAGE_SIZE as u16,
			version: FORMAT_VERSION,
		})
		.as_bytes()
		.to_vec();
		let mut file = File::create(tempdir.path().join("0")).unwrap();
		file.set_len(SEGMENT_SIZE as u64).unwrap();
		file.write_all(&file_start).unwrap();

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
			.write(SegmentWriteOp {
				page_num: non_zero!(3),
				wal_index: wal_index!(69, 420),
				buf: &[3; PAGE_BODY_SIZE],
			})
			.unwrap();

		// then
		let mut file = File::open(tempdir.path().join("0")).unwrap();
		file.seek(SeekFrom::Start((3 * PAGE_SIZE) as u64)).unwrap();
		let received: &mut [u8] = &mut [0; PAGE_SIZE];
		file.read_exact(received).unwrap();

		assert_buf_eq!(
			received,
			[
				PageHeaderRepr {
					wal_generation: 69,
					wal_offset: 420,
					crc: 0x0c78,
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
			.write(SegmentWriteOp {
				page_num: non_zero!(5),
				wal_index: wal_index!(69, 420),
				buf: &[25; PAGE_BODY_SIZE],
			})
			.unwrap();

		// when
		let mut data = [0; PAGE_BODY_SIZE];
		let wal_index = segment
			.read(SegmentReadOp {
				page_num: non_zero!(5),
				buf: &mut data,
			})
			.unwrap();

		// then
		assert_eq!(wal_index, Some(wal_index!(69, 420)));
		assert_eq!(data, [25; PAGE_BODY_SIZE]);
	}
}
