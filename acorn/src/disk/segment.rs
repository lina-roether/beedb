use std::{cell::UnsafeCell, fs::File, io};

use static_assertions::assert_impl_all;
use thiserror::Error;

use crate::{
	consts::{SEGMENT_FORMAT_VERSION, SEGMENT_MAGIC},
	io::IoTarget,
	pages::HeaderPage,
	utils::{
		byte_order::ByteOrder,
		byte_view::{AlignedBuffer, AlignedBytes, ByteView},
		units::display_size,
	},
};

use super::lock::PageLocker;

#[derive(Debug, Error)]
pub enum LoadError {
	#[error("The segment file header is corrupted")]
	CorruptedHeader,

	#[error("This file is not an acorn segment file")]
	NotASegmentFile,

	#[error(
		"Format version mismatch: should be {}, but found {0}",
		SEGMENT_FORMAT_VERSION
	)]
	FormatVersionMismatch(u8),

	#[error("Byte order mismatch: should be {}, but found {0}", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error("Page size mismatch: should be {}, but found {}", display_size(*_1 as usize), display_size(*_0 as usize))]
	PageSizeMismatch(u16, u16),

	#[error(transparent)]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Failed to write the file header completely")]
	IncompleteWrite,

	#[error(transparent)]
	Io(#[from] io::Error),
}

pub struct InitParams {
	pub page_size: u16,
}

pub struct LoadParams {
	pub page_size: u16,
}

pub struct SegmentFile<T: IoTarget> {
	page_size: u16,
	target: UnsafeCell<T>,
	locker: PageLocker,
}

unsafe impl<T: IoTarget> Sync for SegmentFile<T> {}

assert_impl_all!(SegmentFile<File>: Sync);

impl<T: IoTarget> SegmentFile<T> {
	pub fn init(target: &mut T, params: InitParams) -> Result<(), InitError> {
		let mut header_buf: AlignedBytes<12> = Default::default();
		let header = HeaderPage::from_bytes_mut(header_buf.as_mut());
		*header = HeaderPage {
			magic: SEGMENT_MAGIC,
			format_version: SEGMENT_FORMAT_VERSION,
			page_size: params.page_size,
			byte_order: ByteOrder::NATIVE as u8,
			num_pages: 1,
			freelist_trunk: None,
		};
		if target.write_at(header_buf.as_ref(), 0)? != header_buf.len() {
			return Err(InitError::IncompleteWrite);
		}
		Ok(())
	}

	pub fn load(target: T, params: LoadParams) -> Result<Self, LoadError> {
		let mut buf: AlignedBytes<12> = Default::default();
		let bytes_read = target.read_at(buf.as_mut(), 0)?;
		if bytes_read != buf.len() {
			return Err(LoadError::CorruptedHeader);
		}
		let header = HeaderPage::from_bytes(buf.as_ref());
		if header.magic != SEGMENT_MAGIC {
			return Err(LoadError::NotASegmentFile);
		}
		if header.format_version != SEGMENT_FORMAT_VERSION {
			return Err(LoadError::FormatVersionMismatch(header.format_version));
		}
		let Some(byte_order) = ByteOrder::from_byte(header.byte_order) else {
			return Err(LoadError::CorruptedHeader);
		};
		if byte_order != ByteOrder::NATIVE {
			return Err(LoadError::ByteOrderMismatch(byte_order));
		}
		if header.page_size != params.page_size {
			return Err(LoadError::PageSizeMismatch(
				header.page_size,
				params.page_size,
			));
		}
		Ok(Self {
			page_size: header.page_size,
			target: UnsafeCell::new(target),
			locker: PageLocker::new(),
		})
	}

	#[inline]
	pub fn page_size(&self) -> u16 {
		self.page_size
	}

	pub fn read_page(&self, buf: &mut [u8], page_num: u16) -> Result<(), io::Error> {
		self.locker.lock_shared(page_num);
		unsafe {
			(*self.target.get()).read_at(
				&mut buf[0..self.page_size as usize],
				self.offset_of(page_num),
			)?;
			self.locker.unlock_shared(page_num);
		}
		Ok(())
	}

	pub fn write_page(&self, buf: &[u8], page_num: u16) -> Result<(), io::Error> {
		self.locker.lock_exclusive(page_num);
		unsafe {
			(*self.target.get())
				.write_at(&buf[0..self.page_size as usize], self.offset_of(page_num))?;
			self.locker.unlock_exclusive(page_num);
		}
		Ok(())
	}

	fn offset_of(&self, page_num: u16) -> u64 {
		page_num as u64 * self.page_size as u64
	}
}

#[cfg(test)]
mod tests {
	use std::{assert_matches::assert_matches, iter};

	use crate::utils::{byte_view::AlignedBuffer, units::KiB};

	use super::*;

	#[test]
	fn init_file() {
		let mut file = AlignedBuffer::new(8);

		SegmentFile::init(
			&mut file,
			InitParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let header = HeaderPage::from_bytes(&file);
		assert_eq!(header.magic, *b"ACNS");
		assert_eq!(header.format_version, 1);
		assert_eq!(header.byte_order, ByteOrder::NATIVE as u8);
		assert_eq!(header.page_size, 16 * KiB as u16);
		assert_eq!(header.num_pages, 1);
		assert_eq!(header.freelist_trunk, None);
	}

	#[test]
	fn load_file() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ACNS",
			format_version: 1,
			byte_order: ByteOrder::NATIVE as u8,
			page_size: 16 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let segment_file = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		assert_eq!(segment_file.page_size(), 16 * KiB as u16);
	}

	#[test]
	fn try_load_too_short_file() {
		let file = AlignedBuffer::with_capacity(8, 3);

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => assert_matches!(error, LoadError::CorruptedHeader),
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn try_load_file_with_wrong_magic() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ABCD",
			format_version: 1,
			byte_order: ByteOrder::NATIVE as u8,
			page_size: 16 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => assert_matches!(error, LoadError::NotASegmentFile),
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn try_load_file_with_wrong_format_version() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ACNS",
			format_version: 3,
			byte_order: ByteOrder::NATIVE as u8,
			page_size: 16 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => {
				assert_matches!(error, LoadError::FormatVersionMismatch(got) if got == 3)
			}
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn try_load_file_with_invalid_byte_order() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ACNS",
			format_version: 1,
			byte_order: 3,
			page_size: 16 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => {
				assert_matches!(error, LoadError::CorruptedHeader)
			}
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn try_load_file_with_wrong_byte_order() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ACNS",
			format_version: 1,
			byte_order: match ByteOrder::NATIVE {
				ByteOrder::Big => ByteOrder::Little,
				ByteOrder::Little => ByteOrder::Big,
			} as u8,
			page_size: 16 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => {
				assert_matches!(error, LoadError::ByteOrderMismatch(got) if got != ByteOrder::NATIVE)
			}
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn try_load_file_with_wrong_page_size() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		let header = HeaderPage::from_bytes_mut(&mut file);
		*header = HeaderPage {
			magic: *b"ACNS",
			format_version: 1,
			byte_order: ByteOrder::NATIVE as u8,
			page_size: 15 * KiB as u16,
			num_pages: 1,
			freelist_trunk: None,
		};

		let result = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		);

		match result {
			Err(error) => {
				assert_matches!(error, LoadError::PageSizeMismatch(got, want) if got == 15 * KiB as u16 && want == 16 * KiB as u16)
			}
			Ok(..) => panic!("Should not succeed"),
		}
	}

	#[test]
	fn read_page() {
		let mut file = AlignedBuffer::with_capacity(8, 2 * 16 * KiB);
		SegmentFile::init(
			&mut file,
			InitParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		file[16 * KiB..32 * KiB].fill(69);

		let segment_file = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let mut buf: Box<[u8]> = iter::repeat(0).take(16 * KiB).collect();
		segment_file.read_page(&mut buf, 1).unwrap();

		assert!(buf.iter().all(|b| *b == 69));
	}

	#[test]
	fn write_page() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		SegmentFile::init(
			&mut file,
			InitParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let mut segment_file = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let mut buf = AlignedBuffer::with_capacity(8, 16 * KiB);
		buf.fill(25);
		segment_file.write_page(&buf, 2).unwrap();

		assert!(segment_file.target.get_mut()[32 * KiB..48 * KiB]
			.iter()
			.all(|b| *b == 25));
	}

	#[test]
	fn read_nonexistent_page() {
		let mut file = AlignedBuffer::with_capacity(8, 16 * KiB);
		SegmentFile::init(
			&mut file,
			InitParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let segment_file = SegmentFile::load(
			file,
			LoadParams {
				page_size: 16 * KiB as u16,
			},
		)
		.unwrap();

		let mut buf: Box<[u8]> = iter::repeat(0).take(16 * KiB).collect();
		segment_file.read_page(&mut buf, 69).unwrap();

		assert!(buf.iter().all(|b| *b == 0));
	}
}
