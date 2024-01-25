use std::{
	fs::{File, OpenOptions},
	io::{self, BufReader, Read, Seek, SeekFrom, Write},
	iter,
	mem::size_of,
	num::NonZeroU64,
	path::Path,
};

use byte_view::{ByteView, ViewBuf};
use thiserror::Error;

use crate::{
	consts::{DEFAULT_PAGE_SIZE, WAL_MAGIC},
	id::PageId,
	utils::{byte_order::ByteOrder, units::display_size},
};

#[derive(Debug, Error)]
pub(crate) enum InitError {
	#[error(transparent)]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum LoadError {
	#[error("This file is not an acorn WAL file")]
	NotAWalFile,

	#[error("The WAL file is corrupted")]
	Corrupted,

	#[error(
		"Page size mismatch; should be {}, but found {}",
		display_size(*_1 as usize),
		display_size(*_0 as usize)
	)]
	PageSizeMismatch(u16, u16),

	#[error("Byte order mismatch; should be {}, but found {0}", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error(transparent)]
	Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub(crate) enum ReadError {
	#[error("The WAL file is corrupted")]
	Corrupted,

	#[error(transparent)]
	Io(#[from] io::Error),
}

pub(crate) struct InitParams {
	pub page_size: u16,
}

impl Default for InitParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

pub(crate) struct LoadParams {
	pub page_size: u16,
}

impl Default for LoadParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

#[repr(u8)]
enum ItemKind {
	Write = 0,
	Commit = 1,
	Cancel = 2,
}

impl ItemKind {
	fn from_u64(num: u64) -> Result<Self, ReadError> {
		match num {
			0 => Ok(Self::Write),
			1 => Ok(Self::Commit),
			2 => Ok(Self::Cancel),
			_ => Err(ReadError::Corrupted),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, ByteView)]
struct ItemHeader {
	pub kind: u64,
	pub seq: u64,
	pub tid: u64,
}

pub(crate) struct Wal<T: Seek + Read + Write> {
	log_start: u64,
	page_size: u16,
	batch_buf: Vec<u8>,
	file: T,
}

impl Wal<File> {
	pub fn init_file(path: impl AsRef<Path>, params: InitParams) -> Result<(), InitError> {
		let mut file = OpenOptions::new()
			.write(true)
			.truncate(true)
			.create(true)
			.open(path)?;
		Self::init(&mut file, params)
	}

	pub fn load_file(path: impl AsRef<Path>, params: LoadParams) -> Result<Self, LoadError> {
		let file = OpenOptions::new().read(true).append(true).open(path)?;
		Self::load(file, params)
	}

	pub fn clear(&mut self) -> Result<(), io::Error> {
		self.file.set_len(self.log_start)?;
		Ok(())
	}
}

impl<T: Seek + Read + Write> Wal<T> {
	pub fn init(file: &mut T, params: InitParams) -> Result<(), InitError> {
		let mut header: ViewBuf<Header> = ViewBuf::new();
		*header = Header {
			magic: WAL_MAGIC,
			log_start: size_of::<Header>() as u16,
			page_size: params.page_size,
			byte_order: ByteOrder::NATIVE as u8,
		};

		file.seek(SeekFrom::Start(0))?;
		file.write_all(header.as_bytes())?;
		Ok(())
	}

	pub fn load(mut file: T, params: LoadParams) -> Result<Self, LoadError> {
		let mut header: ViewBuf<Header> = ViewBuf::new();
		file.seek(SeekFrom::Start(0))?;
		file.read_exact(header.as_bytes_mut())?;

		if header.magic != WAL_MAGIC {
			return Err(LoadError::NotAWalFile);
		}
		let Some(byte_order) = ByteOrder::from_byte(header.byte_order) else {
			return Err(LoadError::Corrupted);
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

		file.seek(SeekFrom::Start(header.log_start as u64))?;
		Ok(Self {
			file,
			log_start: header.log_start as u64,
			page_size: header.page_size,
			batch_buf: Vec::new(),
		})
	}

	pub fn push_write(
		&mut self,
		tid: u64,
		seq: NonZeroU64,
		page_id: PageId,
		before: &[u8],
		after: &[u8],
	) {
		debug_assert!(before.len() <= self.page_size as usize);
		debug_assert!(after.len() <= self.page_size as usize);

		self.push_header(ItemKind::Write, tid, seq);
		let page_id_buf = ViewBuf::from(page_id);

		self.batch_buf.extend(page_id_buf.as_bytes());

		// The additional extend is there to pad out the data of before and after
		// to the page size
		self.batch_buf.extend(before);
		self.batch_buf
			.extend(iter::repeat(0).take(self.page_size as usize - before.len()));
		self.batch_buf.extend(after);
		self.batch_buf
			.extend(iter::repeat(0).take(self.page_size as usize - after.len()));
	}

	pub fn push_commit(&mut self, tid: u64, seq: NonZeroU64) {
		self.push_header(ItemKind::Commit, tid, seq);
	}

	pub fn push_cancel(&mut self, tid: u64, seq: NonZeroU64) {
		self.push_header(ItemKind::Cancel, tid, seq);
	}

	pub fn flush(&mut self) -> Result<(), io::Error> {
		self.file.write_all(&self.batch_buf)?;
		self.batch_buf.clear();
		Ok(())
	}

	pub fn iter(&mut self) -> Result<Iter<T>, ReadError> {
		self.file.seek(SeekFrom::Start(self.log_start))?;
		Ok(Iter::new(&mut self.file, self.page_size))
	}

	fn push_header(&mut self, kind: ItemKind, tid: u64, seq: NonZeroU64) {
		let header = ViewBuf::from(ItemHeader {
			kind: kind as u64,
			seq: seq.get(),
			tid,
		});
		self.batch_buf.extend(header.as_bytes());
	}
}

#[derive(ByteView, Debug, PartialEq, Eq)]
struct Header {
	magic: [u8; 4],
	log_start: u16,
	page_size: u16,
	byte_order: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item {
	Write {
		tid: u64,
		page_id: PageId,
		before: Box<[u8]>,
		after: Box<[u8]>,
	},
	Commit(u64),
	Cancel(u64),
}

pub(crate) struct Iter<'a, T: Read> {
	page_size: u16,
	seq: u64,
	file: BufReader<&'a mut T>,
}

impl<'a, T: Read> Iter<'a, T> {
	fn new(file: &'a mut T, page_size: u16) -> Self {
		Self {
			file: BufReader::new(file),
			page_size,
			seq: 0,
		}
	}

	fn read_next_item(&mut self) -> Result<Option<Item>, ReadError> {
		let mut header_buf: ViewBuf<ItemHeader> = ViewBuf::new();
		let bytes_read = self.file.read(header_buf.as_bytes_mut())?;
		if bytes_read == 0 {
			// Reached EOF
			return Ok(None);
		} else if bytes_read != header_buf.size() {
			// Junk data at the end :/
			return Err(ReadError::Corrupted);
		}

		if header_buf.seq <= self.seq {
			return Err(ReadError::Corrupted);
		}
		self.seq = header_buf.seq;

		let kind = ItemKind::from_u64(header_buf.kind)?;
		match kind {
			ItemKind::Commit => Ok(Some(Item::Commit(header_buf.tid))),
			ItemKind::Cancel => Ok(Some(Item::Cancel(header_buf.tid))),
			ItemKind::Write => {
				let mut page_id_buf: ViewBuf<PageId> = ViewBuf::new();
				self.file.read_exact(page_id_buf.as_bytes_mut())?;

				let mut before_buf: Box<[u8]> = vec![0; self.page_size.into()].into();
				self.file.read_exact(&mut before_buf)?;

				let mut after_buf: Box<[u8]> = vec![0; self.page_size.into()].into();
				self.file.read_exact(&mut after_buf)?;

				Ok(Some(Item::Write {
					tid: header_buf.tid,
					page_id: *page_id_buf,
					before: before_buf,
					after: after_buf,
				}))
			}
		}
	}
}

impl<'a, T: Read> Iterator for Iter<'a, T> {
	type Item = Result<Item, ReadError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.read_next_item().transpose()
	}
}

#[cfg(test)]
mod tests {
	use std::{alloc::Layout, io::Cursor, num::NonZeroU64};

	use crate::utils::aligned_buf::AlignedBuffer;

	use super::*;

	#[test]
	fn init_wal_file() {
		let mut buf = AlignedBuffer::with_layout(Layout::new::<Header>());
		let mut file = Cursor::new(buf.as_mut());

		Wal::init(&mut file, InitParams { page_size: 1024 }).unwrap();

		let mut expected: ViewBuf<Header> = ViewBuf::new();
		*expected = Header {
			magic: *b"ACNL",
			log_start: 10,
			byte_order: ByteOrder::NATIVE as u8,
			page_size: 1024,
		};
		assert_eq!(Header::from_bytes(&buf), &*expected);
	}

	#[test]
	fn load_wal_file() {
		let mut file = Cursor::new(Vec::<u8>::new());
		Wal::init(&mut file, InitParams { page_size: 1024 }).unwrap();

		Wal::load(file, LoadParams { page_size: 1024 }).unwrap();
	}

	#[test]
	// Miri won't shut up about technically using uninitialized memory in this test; it really
	// doesn't matter in this case though (it's complaining about the padding bytes in
	// WalItemHeader being uninitialized)
	#[cfg_attr(miri, ignore)]
	fn log_items() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file, InitParams { page_size: 8 }).unwrap();

		let mut wal = Wal::load(file, LoadParams { page_size: 8 }).unwrap();
		wal.push_write(
			0,
			NonZeroU64::new(1).unwrap(),
			PageId::new(0, 10),
			&[2; 8],
			&[10; 8],
		);
		wal.push_write(
			0,
			NonZeroU64::new(2).unwrap(),
			PageId::new(0, 12),
			&[0; 8],
			&[15; 8],
		);
		wal.push_commit(0, NonZeroU64::new(3).unwrap());
		wal.flush().unwrap();

		assert_eq!(
			&data[size_of::<Header>()..],
			&[
				ViewBuf::from(ItemHeader {
					kind: ItemKind::Write as u64,
					tid: 0,
					seq: 1,
				})
				.as_bytes(),
				ViewBuf::from(PageId::new(0, 10)).as_bytes(),
				&[2; 8],
				&[10; 8],
				ViewBuf::from(ItemHeader {
					kind: ItemKind::Write as u64,
					tid: 0,
					seq: 2,
				})
				.as_bytes(),
				ViewBuf::from(PageId::new(0, 12)).as_bytes(),
				&[0; 8],
				&[15; 8],
				ViewBuf::from(ItemHeader {
					kind: ItemKind::Commit as u64,
					tid: 0,
					seq: 3,
				})
				.as_bytes()
			]
			.concat()
		);
	}

	#[test]
	fn dont_log_when_not_flushed() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file, InitParams { page_size: 8 }).unwrap();

		let mut wal = Wal::load(file, LoadParams { page_size: 8 }).unwrap();
		wal.push_write(
			0,
			NonZeroU64::new(1).unwrap(),
			PageId::new(0, 10),
			&[2; 8],
			&[10; 8],
		);
		wal.push_write(
			0,
			NonZeroU64::new(2).unwrap(),
			PageId::new(0, 12),
			&[0; 8],
			&[15; 8],
		);
		wal.push_commit(0, NonZeroU64::new(3).unwrap());

		assert!(data[size_of::<Header>()..].is_empty());
	}

	#[test]
	fn iter_logs() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file, InitParams { page_size: 8 }).unwrap();

		let mut wal = Wal::load(file, LoadParams { page_size: 8 }).unwrap();
		wal.push_write(
			0,
			NonZeroU64::new(1).unwrap(),
			PageId::new(0, 10),
			&[0; 8],
			&[10; 8],
		);
		wal.push_write(
			1,
			NonZeroU64::new(2).unwrap(),
			PageId::new(0, 12),
			&[0; 8],
			&[25; 8],
		);
		wal.push_write(
			0,
			NonZeroU64::new(3).unwrap(),
			PageId::new(0, 12),
			&[0; 8],
			&[15; 8],
		);
		wal.push_commit(0, NonZeroU64::new(4).unwrap());
		wal.push_commit(1, NonZeroU64::new(5).unwrap());
		wal.flush().unwrap();

		let mut iter = wal.iter().unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item::Write {
				tid: 0,
				page_id: PageId::new(0, 10),
				before: vec![0; 8].into(),
				after: vec![10; 8].into()
			}
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item::Write {
				tid: 1,
				page_id: PageId::new(0, 12),
				before: vec![0; 8].into(),
				after: vec![25; 8].into()
			}
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item::Write {
				tid: 0,
				page_id: PageId::new(0, 12),
				before: vec![0; 8].into(),
				after: vec![15; 8].into()
			}
		);
		assert_eq!(iter.next().unwrap().unwrap(), Item::Commit(0));
		assert_eq!(iter.next().unwrap().unwrap(), Item::Commit(1));
		assert!(iter.next().is_none());
	}

	#[test]
	fn fail_iter_with_non_monotone_seq() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file, InitParams { page_size: 8 }).unwrap();

		let mut wal = Wal::load(file, LoadParams { page_size: 8 }).unwrap();
		wal.push_write(
			0,
			NonZeroU64::new(1).unwrap(),
			PageId::new(0, 10),
			&[0; 8],
			&[10; 8],
		);
		wal.push_write(
			1,
			NonZeroU64::new(1).unwrap(),
			PageId::new(0, 12),
			&[0; 8],
			&[25; 8],
		);
		wal.flush().unwrap();

		let mut iter = wal.iter().unwrap();
		assert!(iter.next().unwrap().is_ok());
		assert!(iter.next().unwrap().is_err());
	}
}
