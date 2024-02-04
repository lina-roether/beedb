use std::{
	fs::{File, OpenOptions},
	io::{self, BufReader, Read, Seek, SeekFrom, Write},
	mem::size_of,
	num::NonZeroU64,
	path::Path,
};

use byte_view::{ByteView, ViewBuf, ViewSlice};
use thiserror::Error;

use crate::{
	consts::WAL_MAGIC,
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

	#[error("WAL is missing item with seq {0}")]
	MissingSeq(NonZeroU64),

	#[error(transparent)]
	Io(#[from] io::Error),
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
	length: u64,
	kind: u64,
	seq: Option<NonZeroU64>,
	prev_seq: Option<NonZeroU64>,
	tid: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, ByteView)]
struct ItemFooter {
	length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemInfo {
	pub tid: u64,
	pub seq: NonZeroU64,
	pub prev_seq: Option<NonZeroU64>,
}

pub(crate) struct WriteInfo<'a> {
	pub page_id: PageId,
	pub start: u16,
	pub before: &'a [u8],
	pub after: &'a [u8],
}

pub(crate) struct Wal<T: Seek + Read + Write> {
	log_start: u64,
	batch_buf: Vec<u8>,
	file: T,
}

impl Wal<File> {
	pub fn init_file(path: impl AsRef<Path>) -> Result<(), InitError> {
		let mut file = OpenOptions::new()
			.write(true)
			.truncate(true)
			.create(true)
			.open(path)?;
		Self::init(&mut file)
	}

	pub fn load_file(path: impl AsRef<Path>) -> Result<Self, LoadError> {
		let file = OpenOptions::new().read(true).append(true).open(path)?;
		Self::load(file)
	}

	pub fn clear(&mut self) -> Result<(), io::Error> {
		self.file.set_len(self.log_start)?;
		Ok(())
	}
}

impl<T: Seek + Read + Write> Wal<T> {
	const EMPTY_ITEM_LENGTH: usize = Self::get_length(0);

	pub fn init(file: &mut T) -> Result<(), InitError> {
		let mut header: ViewBuf<Header> = ViewBuf::new();
		*header = Header {
			magic: WAL_MAGIC,
			log_start: size_of::<Header>() as u16,
			byte_order: ByteOrder::NATIVE as u8,
		};

		file.seek(SeekFrom::Start(0))?;
		file.write_all(header.as_bytes())?;
		Ok(())
	}

	pub fn load(mut file: T) -> Result<Self, LoadError> {
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

		file.seek(SeekFrom::Start(header.log_start as u64))?;
		Ok(Self {
			file,
			log_start: header.log_start as u64,
			batch_buf: Vec::new(),
		})
	}

	pub fn push_write(
		&mut self,
		item_info: ItemInfo,
		WriteInfo {
			page_id,
			start,
			before,
			after,
		}: WriteInfo,
	) -> Result<(), io::Error> {
		debug_assert_eq!(after.len(), before.len());

		self.file.seek(SeekFrom::End(0))?;

		let length = Self::get_length(size_of::<WriteItemHeader>() + before.len() * 2);

		self.push_header(length, ItemKind::Write, item_info);

		let write_header = WriteItemHeader {
			page_id,
			start,
			len: before
				.len()
				.try_into()
				.expect("Write operations must have a 16-bit length!"),
		};
		self.batch_buf
			.extend(ViewSlice::from(&write_header).as_bytes());

		self.batch_buf.extend(before);
		self.batch_buf.extend(&after[0..before.len()]);

		self.push_footer(length);

		Ok(())
	}

	pub fn push_commit(&mut self, item_info: ItemInfo) -> Result<(), io::Error> {
		self.file.seek(SeekFrom::End(0))?;
		self.push_header(Self::EMPTY_ITEM_LENGTH, ItemKind::Commit, item_info);
		self.push_footer(Self::EMPTY_ITEM_LENGTH);
		Ok(())
	}

	pub fn push_cancel(&mut self, item_info: ItemInfo) -> Result<(), io::Error> {
		self.file.seek(SeekFrom::End(0))?;
		self.push_header(Self::EMPTY_ITEM_LENGTH, ItemKind::Cancel, item_info);
		self.push_footer(Self::EMPTY_ITEM_LENGTH);
		Ok(())
	}

	pub fn flush(&mut self) -> Result<(), io::Error> {
		self.file.write_all(&self.batch_buf)?;
		self.batch_buf.clear();
		Ok(())
	}

	pub fn iter(&mut self) -> Result<Iter<T>, ReadError> {
		Iter::new(&mut self.file, self.log_start)
	}

	pub fn retrace_transaction(&mut self, seq: NonZeroU64) -> Result<Retrace<T>, ReadError> {
		Retrace::new(&mut self.file, self.log_start, seq)
	}

	#[inline]
	const fn get_length(content_length: usize) -> usize {
		size_of::<ItemHeader>() + content_length + size_of::<ItemFooter>()
	}

	fn push_header(
		&mut self,
		length: usize,
		kind: ItemKind,
		ItemInfo { tid, seq, prev_seq }: ItemInfo,
	) {
		let header = ItemHeader {
			length: length as u64,
			kind: kind as u64,
			seq: Some(seq),
			prev_seq,
			tid,
		};
		self.batch_buf.extend(ViewSlice::from(&header).as_bytes());
	}

	fn push_footer(&mut self, length: usize) {
		let footer = ItemFooter {
			length: length as u64,
		};
		self.batch_buf.extend(ViewSlice::from(&footer).as_bytes())
	}
}

#[derive(ByteView, Debug, PartialEq, Eq)]
struct Header {
	magic: [u8; 4],
	log_start: u16,
	byte_order: u8,
}

#[derive(ByteView, Debug, PartialEq, Eq)]
struct WriteItemHeader {
	page_id: PageId,
	start: u16,
	len: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ItemData {
	Write {
		page_id: PageId,
		start: u16,
		before: Box<[u8]>,
		after: Box<[u8]>,
	},
	Commit,
	Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
	pub info: ItemInfo,
	pub data: ItemData,
}

struct WalReader<'a, T: Read + Seek> {
	log_start: u64,
	file: BufReader<&'a mut T>,
}

impl<'a, T: Read + Seek> WalReader<'a, T> {
	fn new(file: &'a mut T, log_start: u64) -> Self {
		Self {
			log_start,
			file: BufReader::new(file),
		}
	}

	fn seek_back_to_seq(&mut self, seq: NonZeroU64) -> Result<(), ReadError> {
		let mut header_buf: ViewBuf<ItemHeader> = ViewBuf::new();
		let mut footer_buf: ViewBuf<ItemFooter> = ViewBuf::new();

		while self.file.stream_position()? > self.log_start {
			self.file.seek_relative(-(size_of::<ItemFooter>() as i64))?;
			self.file.read_exact(footer_buf.as_bytes_mut())?;
			self.file.seek_relative(-(footer_buf.length as i64))?;
			self.file.read_exact(header_buf.as_bytes_mut())?;
			self.file.seek_relative(-(size_of::<ItemHeader>() as i64))?;
			if header_buf.seq == Some(seq) {
				return Ok(());
			}
		}

		Err(ReadError::MissingSeq(seq))
	}

	fn read_next_item(&mut self, advance: bool) -> Result<Option<Item>, ReadError> {
		let mut header_buf: ViewBuf<ItemHeader> = ViewBuf::new();
		let bytes_read = self.file.read(header_buf.as_bytes_mut())?;
		if bytes_read == 0 {
			// EOF
			return Ok(None);
		} else if bytes_read != header_buf.size() {
			// Junk data
			return Err(ReadError::Corrupted);
		}

		let kind = ItemKind::from_u64(header_buf.kind)?;
		let data = match kind {
			ItemKind::Commit => ItemData::Commit,
			ItemKind::Cancel => ItemData::Cancel,
			ItemKind::Write => {
				let mut write_header_buf: ViewBuf<WriteItemHeader> = ViewBuf::new();
				self.file.read_exact(write_header_buf.as_bytes_mut())?;

				let mut before_buf: Box<[u8]> = vec![0; write_header_buf.len.into()].into();
				self.file.read_exact(&mut before_buf)?;

				let mut after_buf: Box<[u8]> = vec![0; write_header_buf.len.into()].into();
				self.file.read_exact(&mut after_buf)?;

				ItemData::Write {
					page_id: write_header_buf.page_id,
					start: write_header_buf.start,
					before: before_buf,
					after: after_buf,
				}
			}
		};

		self.file.seek_relative(size_of::<ItemFooter>() as i64)?;

		if !advance {
			self.file.seek_relative(-(header_buf.length as i64))?;
		}

		Ok(Some(Item {
			info: ItemInfo {
				tid: header_buf.tid,
				seq: header_buf.seq.ok_or(ReadError::Corrupted)?,
				prev_seq: header_buf.prev_seq,
			},
			data,
		}))
	}
}

pub(crate) struct Iter<'a, T: Read + Seek>(WalReader<'a, T>);

impl<'a, T: Read + Seek> Iter<'a, T> {
	fn new(file: &'a mut T, log_start: u64) -> Result<Self, ReadError> {
		file.seek(SeekFrom::Start(log_start))?;
		Ok(Self(WalReader::new(file, log_start)))
	}
}

impl<'a, T: Read + Seek> Iterator for Iter<'a, T> {
	type Item = Result<Item, ReadError>;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.read_next_item(true).transpose()
	}
}

pub(crate) struct Retrace<'a, T: Read + Seek> {
	seq: Option<NonZeroU64>,
	reader: WalReader<'a, T>,
}

impl<'a, T: Read + Seek> Retrace<'a, T> {
	fn new(file: &'a mut T, log_start: u64, seq: NonZeroU64) -> Result<Self, ReadError> {
		file.seek(SeekFrom::End(0))?;

		Ok(Self {
			seq: Some(seq),
			reader: WalReader::new(file, log_start),
		})
	}
}

impl<'a, T: Read + Seek> Iterator for Retrace<'a, T> {
	type Item = Result<Item, ReadError>;

	fn next(&mut self) -> Option<Self::Item> {
		if let Err(err) = self.reader.seek_back_to_seq(self.seq?) {
			return Some(Err(err));
		}

		let item = match self.reader.read_next_item(false) {
			Ok(item) => item.unwrap(),
			Err(err) => return Some(Err(err)),
		};
		self.seq = item.info.prev_seq;
		Some(Ok(item))
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

		Wal::init(&mut file).unwrap();

		let mut expected: ViewBuf<Header> = ViewBuf::new();
		*expected = Header {
			magic: *b"ACNL",
			log_start: 8,
			byte_order: ByteOrder::NATIVE as u8,
		};
		assert_eq!(Header::from_bytes(&buf), &*expected);
	}

	#[test]
	fn load_wal_file() {
		let mut file = Cursor::new(Vec::<u8>::new());
		Wal::init(&mut file).unwrap();

		Wal::load(file).unwrap();
	}

	#[test]
	fn log_items() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file).unwrap();

		let mut wal = Wal::load(file).unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(1).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[2; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(2).unwrap(),
				prev_seq: NonZeroU64::new(1),
			},
			WriteInfo {
				page_id: PageId::new(0, 12),
				start: 0,
				before: &[69; 8],
				after: &[0; 8],
			},
		)
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 0,
			seq: NonZeroU64::new(3).unwrap(),
			prev_seq: NonZeroU64::new(2),
		})
		.unwrap();
		wal.flush().unwrap();

		assert_eq!(
			&data[size_of::<Header>()..],
			&[
				ViewSlice::from(&ItemHeader {
					length: 76,
					kind: ItemKind::Write as u64,
					tid: 0,
					seq: NonZeroU64::new(1),
					prev_seq: None,
				})
				.as_bytes(),
				ViewSlice::from(&WriteItemHeader {
					page_id: PageId::new(0, 10),
					start: 0,
					len: 8
				})
				.as_bytes(),
				&[0; 8],
				&[2; 8],
				ViewSlice::from(&ItemFooter { length: 76 }).as_bytes(),
				ViewSlice::from(&ItemHeader {
					length: 76,
					kind: ItemKind::Write as u64,
					tid: 0,
					seq: NonZeroU64::new(2),
					prev_seq: NonZeroU64::new(1)
				})
				.as_bytes(),
				ViewSlice::from(&WriteItemHeader {
					page_id: PageId::new(0, 12),
					start: 0,
					len: 8
				})
				.as_bytes(),
				&[69; 8],
				&[0; 8],
				ViewSlice::from(&ItemFooter { length: 76 }).as_bytes(),
				ViewSlice::from(&ItemHeader {
					length: 48,
					kind: ItemKind::Commit as u64,
					tid: 0,
					seq: NonZeroU64::new(3),
					prev_seq: NonZeroU64::new(2)
				})
				.as_bytes(),
				ViewSlice::from(&ItemFooter { length: 48 }).as_bytes(),
			]
			.concat()
		);
	}

	#[test]
	fn dont_log_when_not_flushed() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file).unwrap();

		let mut wal = Wal::load(file).unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(1).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[2; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(2).unwrap(),
				prev_seq: NonZeroU64::new(1),
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[0; 8],
			},
		)
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 0,
			seq: NonZeroU64::new(3).unwrap(),
			prev_seq: NonZeroU64::new(2),
		})
		.unwrap();

		assert!(data[size_of::<Header>()..].is_empty());
	}

	#[test]
	fn iter_logs() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file).unwrap();

		let mut wal = Wal::load(file).unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(1).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[10; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 1,
				seq: NonZeroU64::new(2).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[25; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(3).unwrap(),
				prev_seq: NonZeroU64::new(1),
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[69; 8],
				after: &[15; 8],
			},
		)
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 0,
			seq: NonZeroU64::new(4).unwrap(),
			prev_seq: NonZeroU64::new(3),
		})
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 1,
			seq: NonZeroU64::new(5).unwrap(),
			prev_seq: NonZeroU64::new(2),
		})
		.unwrap();
		wal.flush().unwrap();

		let mut iter = wal.iter().unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 0,
					seq: NonZeroU64::new(1).unwrap(),
					prev_seq: None
				},
				data: ItemData::Write {
					page_id: PageId::new(0, 10),
					start: 0,
					before: vec![0; 8].into(),
					after: vec![10; 8].into()
				}
			},
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 1,
					seq: NonZeroU64::new(2).unwrap(),
					prev_seq: None
				},
				data: ItemData::Write {
					page_id: PageId::new(0, 10),
					start: 0,
					before: vec![0; 8].into(),
					after: vec![25; 8].into()
				}
			},
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 0,
					seq: NonZeroU64::new(3).unwrap(),
					prev_seq: NonZeroU64::new(1)
				},
				data: ItemData::Write {
					page_id: PageId::new(0, 10),
					start: 0,
					before: vec![69; 8].into(),
					after: vec![15; 8].into()
				}
			},
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 0,
					seq: NonZeroU64::new(4).unwrap(),
					prev_seq: NonZeroU64::new(3)
				},
				data: ItemData::Commit
			}
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 1,
					seq: NonZeroU64::new(5).unwrap(),
					prev_seq: NonZeroU64::new(2)
				},
				data: ItemData::Commit
			}
		);
		assert!(iter.next().is_none());
	}

	#[test]
	fn retrace_transaction() {
		let mut data: Vec<u8> = Vec::new();
		let mut file = Cursor::new(&mut data);
		Wal::init(&mut file).unwrap();

		let mut wal = Wal::load(file).unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(1).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[10; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 1,
				seq: NonZeroU64::new(2).unwrap(),
				prev_seq: None,
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[0; 8],
				after: &[25; 8],
			},
		)
		.unwrap();
		wal.push_write(
			ItemInfo {
				tid: 0,
				seq: NonZeroU64::new(3).unwrap(),
				prev_seq: NonZeroU64::new(1),
			},
			WriteInfo {
				page_id: PageId::new(0, 10),
				start: 0,
				before: &[69; 8],
				after: &[15; 8],
			},
		)
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 0,
			seq: NonZeroU64::new(4).unwrap(),
			prev_seq: NonZeroU64::new(3),
		})
		.unwrap();
		wal.push_commit(ItemInfo {
			tid: 1,
			seq: NonZeroU64::new(5).unwrap(),
			prev_seq: NonZeroU64::new(2),
		})
		.unwrap();
		wal.flush().unwrap();

		let mut iter = wal
			.retrace_transaction(NonZeroU64::new(5).unwrap())
			.unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 1,
					seq: NonZeroU64::new(5).unwrap(),
					prev_seq: NonZeroU64::new(2)
				},
				data: ItemData::Commit
			}
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			Item {
				info: ItemInfo {
					tid: 1,
					seq: NonZeroU64::new(2).unwrap(),
					prev_seq: None
				},
				data: ItemData::Write {
					page_id: PageId::new(0, 10),
					start: 0,
					before: vec![0; 8].into(),
					after: vec![25; 8].into()
				}
			},
		);
		assert!(iter.next().is_none());
	}
}
