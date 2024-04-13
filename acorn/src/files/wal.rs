use std::{
	borrow::Cow,
	io::{self, BufReader, Read, Seek, SeekFrom, Write},
	iter,
	mem::size_of,
};

use mockall::automock;
use musli_zerocopy::{OwnedBuf, Ref, ZeroCopy};

use crate::{files::CRC16, utils::KIB};

use super::{FileError, FileType, GenericHeader, GenericHeaderInit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ZeroCopy)]
#[repr(u8)]
pub(crate) enum WalItemKind {
	Write = 0,
	Commit = 1,
	Undo = 2,
}

#[derive(Debug, ZeroCopy)]
#[repr(C)]
struct ItemHeader {
	kind: WalItemKind,
	item_length: u16,
	transaction_id: u64,
	sequence_num: u64,
}

#[derive(Debug, ZeroCopy)]
#[repr(C)]
struct ItemFooter {
	item_length: u16,
}

#[derive(Debug, ZeroCopy)]
#[repr(C)]
struct WriteItemHeader {
	segment_id: u32,
	page_id: u16,
	offset: u16,
	write_length: u16,
	crc: u16,
}

pub(crate) struct WalFile<F: Seek + Read + Write> {
	body_start: u64,
	file: F,
	is_at_end: bool,
	buffer: OwnedBuf,
}

impl<F: Seek + Read + Write> WalFile<F> {
	fn create(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let mut meta = GenericHeader::new(GenericHeaderInit {
			file_type: FileType::Wal,
			header_size: 0,
		});
		file.write_all(meta.to_bytes())?;

		Ok(Self {
			file,
			body_start: meta.content_offset.into(),
			is_at_end: true,
			buffer: OwnedBuf::with_capacity(8 * KIB),
		})
	}

	fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let mut header_buf = OwnedBuf::with_alignment::<GenericHeader>();
		header_buf.store_uninit::<GenericHeader>();
		file.read_exact(header_buf.as_mut_slice())?;

		let header: &GenericHeader = header_buf.load_at(0)?;
		header.validate()?;
		if header.file_type != FileType::Wal {
			return Err(FileError::WrongFileType(header.file_type));
		}
		Ok(Self {
			body_start: header.content_offset.into(),
			file,
			is_at_end: false,
			buffer: OwnedBuf::with_capacity(8 * KIB),
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalWriteItemData<'a> {
	pub segment_id: u32,
	pub page_id: u16,
	pub offset: u16,
	pub data: Cow<'a, [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalItemData<'a> {
	Write(WalWriteItemData<'a>),
	Commit,
	Undo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalItem<'a> {
	pub transaction_id: u64,
	pub sequence_num: u64,
	pub data: WalItemData<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalItemMeta {
	pub transaction_id: u64,
	pub sequence_num: u64,
	pub kind: WalItemKind,
}

#[automock(
    type Cursor = MockWalCursorApi;
)]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	type Cursor<'a>: WalCursorApi
	where
		Self: 'a;

	fn push_item<'a>(&mut self, item: WalItem<'a>) -> Result<(), FileError>;
	fn cursor<'a>(&'a mut self) -> Self::Cursor<'a>;
}

impl<F: Seek + Read + Write> WalFileApi for WalFile<F> {
	type Cursor<'a> = WalCursor<'a, F> where F: 'a;

	fn push_item(&mut self, item: WalItem) -> Result<(), FileError> {
		if !self.is_at_end {
			self.file.seek(SeekFrom::End(0))?;
		}

		self.buffer.clear();

		let kind: WalItemKind = match &item.data {
			WalItemData::Write(..) => WalItemKind::Write,
			WalItemData::Commit => WalItemKind::Commit,
			WalItemData::Undo => WalItemKind::Undo,
		};

		let body_size: usize = match &item.data {
			WalItemData::Write(write_data) => size_of::<WriteItemHeader>() + write_data.data.len(),
			_ => 0,
		};
		let item_length: u16 = (size_of::<ItemHeader>() + body_size + size_of::<ItemFooter>())
			.try_into()
			.expect("WAL item length must be a 16-bit number!");

		self.buffer.store(&ItemHeader {
			kind,
			item_length,
			transaction_id: item.transaction_id,
			sequence_num: item.sequence_num,
		});

		if let WalItemData::Write(write_data) = &item.data {
			let write_item_header = WriteItemHeader {
				segment_id: write_data.segment_id,
				page_id: write_data.page_id,
				offset: write_data.offset,
				write_length: write_data
					.data
					.len()
					.try_into()
					.expect("Data length written in WAL item must be a 16-bit number!"),
				crc: CRC16.checksum(&write_data.data),
			};
			self.buffer.store(&write_item_header);
			self.buffer.store_unsized::<[u8]>(&write_data.data);
		}

		self.buffer.store(&ItemFooter { item_length });

		self.file.write_all(self.buffer.as_slice())?;
		Ok(())
	}

	fn cursor(&mut self) -> Self::Cursor<'_> {
		WalCursor::new(&mut self.file, self.body_start, &mut self.is_at_end)
	}
}

#[automock]
pub(crate) trait WalCursorApi {
	fn seek_to_back(&mut self) -> Result<(), FileError>;
	fn seek_to_front(&mut self) -> Result<(), FileError>;
	fn read_next<'a>(&mut self) -> Result<Option<WalItem<'a>>, FileError>;
	fn read_next_meta(&mut self) -> Result<Option<WalItemMeta>, FileError>;
	fn read_prev<'a>(&mut self) -> Result<Option<WalItem<'a>>, FileError>;
	fn read_prev_meta(&mut self) -> Result<Option<WalItemMeta>, FileError>;
}

pub struct WalCursor<'a, F: Seek + Read> {
	file: BufReader<&'a mut F>,
	start_pos: u64,
	buffer: OwnedBuf,
	is_at_end: &'a mut bool,
}

impl<'a, F: Seek + Read> WalCursor<'a, F> {
	const BUFFER_CAP: usize = 4 * KIB;

	fn new(file: &'a mut F, start_pos: u64, is_at_end: &'a mut bool) -> Self {
		Self {
			file: BufReader::new(file),
			start_pos,
			buffer: OwnedBuf::with_capacity(Self::BUFFER_CAP),
			is_at_end,
		}
	}

	fn seek_to_prev(&mut self) -> Result<bool, FileError> {
		if self.file.stream_position()? <= self.start_pos {
			return Ok(false);
		}

		self.file.seek_relative(-(size_of::<ItemFooter>() as i64))?;
		let footer_ref: Ref<ItemFooter> = read_exact_from(&mut self.buffer, &mut self.file)?;
		let footer = self.buffer.load(footer_ref)?;
		self.file.seek_relative(-(footer.item_length as i64))?;
		Ok(true)
	}

	fn read_write_item_data(&mut self) -> Result<WalWriteItemData<'static>, FileError> {
		let write_header_ref: Ref<WriteItemHeader> =
			read_exact_from(&mut self.buffer, &mut self.file)?;
		let write_header = self.buffer.load(write_header_ref)?;

		let mut data: Vec<u8> = iter::repeat(0)
			.take(write_header.write_length.into())
			.collect();
		self.file.read_exact(&mut data)?;

		if CRC16.checksum(&data) != write_header.crc {
			return Err(FileError::ChecksumMismatch);
		}

		Ok(WalWriteItemData {
			segment_id: write_header.segment_id,
			page_id: write_header.page_id,
			offset: write_header.offset,
			data: Cow::Owned(data),
		})
	}
}

impl<'a, F: Seek + Read> WalCursorApi for WalCursor<'a, F> {
	fn seek_to_back(&mut self) -> Result<(), FileError> {
		self.file.seek(SeekFrom::Start(self.start_pos))?;
		*self.is_at_end = false;
		Ok(())
	}

	fn seek_to_front(&mut self) -> Result<(), FileError> {
		self.file.seek(SeekFrom::End(0))?;
		*self.is_at_end = false;
		Ok(())
	}

	fn read_next<'b>(&mut self) -> Result<Option<WalItem<'b>>, FileError> {
		self.buffer.clear();

		let Some(header_ref) = read_from::<ItemHeader>(&mut self.buffer, &mut self.file)? else {
			return Ok(None);
		};
		let header = self.buffer.load(header_ref)?;
		let transaction_id = header.transaction_id;
		let sequence_num = header.sequence_num;

		let data = match header.kind {
			WalItemKind::Commit => WalItemData::Commit,
			WalItemKind::Undo => WalItemData::Undo,
			WalItemKind::Write => WalItemData::Write(self.read_write_item_data()?),
		};

		self.file
			.seek_relative(size_of::<ItemFooter>().try_into().unwrap())?;

		Ok(Some(WalItem {
			transaction_id,
			sequence_num,
			data,
		}))
	}

	fn read_next_meta(&mut self) -> Result<Option<WalItemMeta>, FileError> {
		self.buffer.clear();

		let Some(header_ref) = read_from::<ItemHeader>(&mut self.buffer, &mut self.file)? else {
			return Ok(None);
		};
		let header = self.buffer.load(header_ref)?;

		self.file
			.seek_relative(header.item_length as i64 - size_of::<ItemHeader>() as i64)?;

		Ok(Some(WalItemMeta {
			kind: header.kind,
			transaction_id: header.transaction_id,
			sequence_num: header.sequence_num,
		}))
	}

	fn read_prev<'b>(&mut self) -> Result<Option<WalItem<'b>>, FileError> {
		if !self.seek_to_prev()? {
			return Ok(None);
		}
		let item = self.read_next()?;
		self.seek_to_prev()?;
		Ok(item)
	}

	fn read_prev_meta(&mut self) -> Result<Option<WalItemMeta>, FileError> {
		if !self.seek_to_prev()? {
			return Ok(None);
		}
		let meta = self.read_next_meta()?;
		self.seek_to_prev()?;
		Ok(meta)
	}
}

fn read_from<T: ZeroCopy>(
	buf: &mut OwnedBuf,
	mut read: impl Read,
) -> Result<Option<Ref<T>>, io::Error> {
	let read_ref = buf.store_uninit::<T>();
	let slice = &mut buf.as_mut_slice()[read_ref.offset()..read_ref.offset() + size_of::<T>()];
	let bytes_read = read.read(slice)?;
	if bytes_read != size_of::<T>() {
		return Ok(None);
	}
	Ok(Some(read_ref.assume_init()))
}

fn read_exact_from<T: ZeroCopy>(
	buf: &mut OwnedBuf,
	mut read: impl Read,
) -> Result<Ref<T>, io::Error> {
	let read_ref = buf.store_uninit::<T>();
	let slice = &mut buf.as_mut_slice()[read_ref.offset()..read_ref.offset() + size_of::<T>()];
	read.read_exact(slice)?;
	Ok(read_ref.assume_init())
}

#[cfg(test)]
mod tests {
	use std::{io::Cursor, mem::size_of};

	use super::*;

	#[test]
	fn create_wal_file() {
		// when
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		WalFile::create(&mut file).unwrap();

		// then
		let mut buf = OwnedBuf::new();
		buf.extend_from_slice(&file.into_inner());
		let header: &GenericHeader = buf.load_at(0).unwrap();
		assert_eq!(header.file_type, FileType::Wal);
		assert_eq!(header.content_offset, size_of::<GenericHeader>() as u16);
	}

	#[test]
	fn open_wal_file() {
		// when
		let mut buf = OwnedBuf::new();
		buf.store(&GenericHeader::new(GenericHeaderInit {
			file_type: FileType::Wal,
			header_size: 0,
		}));

		// then
		let file = Cursor::new(Vec::from(buf.as_mut_slice()));
		WalFile::open(file).unwrap();
	}

	#[test]
	fn push_wal_write_item() {
		// given
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		let mut wal = WalFile::create(&mut file).unwrap();

		// when
		wal.push_item(WalItem {
			transaction_id: 69,
			sequence_num: 420,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 25,
				page_id: 24,
				offset: 12,
				data: Cow::Owned(vec![1, 2, 3, 4]),
			}),
		})
		.unwrap();

		// then
		let expected = [
			ItemHeader {
				kind: WalItemKind::Write,
				item_length: 42,
				transaction_id: 69,
				sequence_num: 420,
			}
			.to_bytes(),
			WriteItemHeader {
				segment_id: 25,
				page_id: 24,
				offset: 12,
				write_length: 4,
				crc: 0x3991,
			}
			.to_bytes(),
			&[1, 2, 3, 4],
			ItemFooter { item_length: 42 }.to_bytes(),
		]
		.concat();

		assert_eq!(file.into_inner()[size_of::<GenericHeader>()..], expected);
	}

	#[test]
	fn push_wal_commit_item() {
		// given
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		let mut wal = WalFile::create(&mut file).unwrap();

		// when
		wal.push_item(WalItem {
			transaction_id: 69,
			sequence_num: 420,
			data: WalItemData::Commit,
		})
		.unwrap();

		// then
		let expected = [
			ItemHeader {
				kind: WalItemKind::Commit,
				item_length: 26,
				transaction_id: 69,
				sequence_num: 420,
			}
			.to_bytes(),
			ItemFooter { item_length: 26 }.to_bytes(),
		]
		.concat();

		assert_eq!(file.into_inner()[size_of::<GenericHeader>()..], expected);
	}

	#[test]
	fn read_items_back_to_front() {
		// given
		let mut wal = WalFile::create(Cursor::new(Vec::new())).unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 0,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 69,
				page_id: 420,
				offset: 25,
				data: vec![1, 2, 3, 4].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 1,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 123,
				page_id: 456,
				offset: 24,
				data: vec![5, 6, 7, 8].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 2,
			data: WalItemData::Undo,
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 3,
			data: WalItemData::Commit,
		})
		.unwrap();

		// when
		let mut cursor = wal.cursor();
		cursor.seek_to_back().unwrap();
		let item_1 = cursor.read_next().unwrap();
		let item_2 = cursor.read_next().unwrap();
		let item_3 = cursor.read_next().unwrap();
		let item_4 = cursor.read_next().unwrap();
		let item_5 = cursor.read_next().unwrap();

		// then
		assert_eq!(
			item_1,
			Some(WalItem {
				transaction_id: 0,
				sequence_num: 0,
				data: WalItemData::Write(WalWriteItemData {
					segment_id: 69,
					page_id: 420,
					offset: 25,
					data: vec![1, 2, 3, 4].into(),
				}),
			})
		);
		assert_eq!(
			item_2,
			Some(WalItem {
				transaction_id: 1,
				sequence_num: 1,
				data: WalItemData::Write(WalWriteItemData {
					segment_id: 123,
					page_id: 456,
					offset: 24,
					data: vec![5, 6, 7, 8].into(),
				}),
			})
		);
		assert_eq!(
			item_3,
			Some(WalItem {
				transaction_id: 1,
				sequence_num: 2,
				data: WalItemData::Undo,
			})
		);
		assert_eq!(
			item_4,
			Some(WalItem {
				transaction_id: 0,
				sequence_num: 3,
				data: WalItemData::Commit,
			})
		);
		assert_eq!(item_5, None);
	}

	#[test]
	fn read_items_front_to_back() {
		// given
		let mut wal = WalFile::create(Cursor::new(Vec::new())).unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 0,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 69,
				page_id: 420,
				offset: 25,
				data: vec![1, 2, 3, 4].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 1,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 123,
				page_id: 456,
				offset: 24,
				data: vec![5, 6, 7, 8].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 2,
			data: WalItemData::Undo,
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 3,
			data: WalItemData::Commit,
		})
		.unwrap();

		// when
		let mut cursor = wal.cursor();
		cursor.seek_to_front().unwrap();
		let item_1 = cursor.read_prev().unwrap();
		let item_2 = cursor.read_prev().unwrap();
		let item_3 = cursor.read_prev().unwrap();
		let item_4 = cursor.read_prev().unwrap();
		let item_5 = cursor.read_prev().unwrap();

		// then
		assert_eq!(
			item_1,
			Some(WalItem {
				transaction_id: 0,
				sequence_num: 3,
				data: WalItemData::Commit,
			})
		);
		assert_eq!(
			item_2,
			Some(WalItem {
				transaction_id: 1,
				sequence_num: 2,
				data: WalItemData::Undo,
			})
		);
		assert_eq!(
			item_3,
			Some(WalItem {
				transaction_id: 1,
				sequence_num: 1,
				data: WalItemData::Write(WalWriteItemData {
					segment_id: 123,
					page_id: 456,
					offset: 24,
					data: vec![5, 6, 7, 8].into(),
				}),
			})
		);
		assert_eq!(
			item_4,
			Some(WalItem {
				transaction_id: 0,
				sequence_num: 0,
				data: WalItemData::Write(WalWriteItemData {
					segment_id: 69,
					page_id: 420,
					offset: 25,
					data: vec![1, 2, 3, 4].into(),
				}),
			})
		);
		assert_eq!(item_5, None);
	}

	#[test]
	fn read_item_meta_back_to_front() {
		// given
		let mut wal = WalFile::create(Cursor::new(Vec::new())).unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 0,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 69,
				page_id: 420,
				offset: 25,
				data: vec![1, 2, 3, 4].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 1,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 123,
				page_id: 456,
				offset: 24,
				data: vec![5, 6, 7, 8].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 2,
			data: WalItemData::Undo,
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 3,
			data: WalItemData::Commit,
		})
		.unwrap();

		// when
		let mut cursor = wal.cursor();
		cursor.seek_to_back().unwrap();
		let item_1 = cursor.read_next_meta().unwrap();
		let item_2 = cursor.read_next_meta().unwrap();
		let item_3 = cursor.read_next_meta().unwrap();
		let item_4 = cursor.read_next_meta().unwrap();
		let item_5 = cursor.read_next_meta().unwrap();

		// then
		assert_eq!(
			item_1,
			Some(WalItemMeta {
				kind: WalItemKind::Write,
				transaction_id: 0,
				sequence_num: 0,
			})
		);
		assert_eq!(
			item_2,
			Some(WalItemMeta {
				kind: WalItemKind::Write,
				transaction_id: 1,
				sequence_num: 1,
			})
		);
		assert_eq!(
			item_3,
			Some(WalItemMeta {
				kind: WalItemKind::Undo,
				transaction_id: 1,
				sequence_num: 2,
			})
		);
		assert_eq!(
			item_4,
			Some(WalItemMeta {
				kind: WalItemKind::Commit,
				transaction_id: 0,
				sequence_num: 3,
			})
		);
		assert_eq!(item_5, None);
	}

	#[test]
	fn read_item_meta_front_to_back() {
		// given
		let mut wal = WalFile::create(Cursor::new(Vec::new())).unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 0,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 69,
				page_id: 420,
				offset: 25,
				data: vec![1, 2, 3, 4].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 1,
			data: WalItemData::Write(WalWriteItemData {
				segment_id: 123,
				page_id: 456,
				offset: 24,
				data: vec![5, 6, 7, 8].into(),
			}),
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 1,
			sequence_num: 2,
			data: WalItemData::Undo,
		})
		.unwrap();
		wal.push_item(WalItem {
			transaction_id: 0,
			sequence_num: 3,
			data: WalItemData::Commit,
		})
		.unwrap();

		// when
		let mut cursor = wal.cursor();
		cursor.seek_to_front().unwrap();
		let item_1 = cursor.read_prev_meta().unwrap();
		let item_2 = cursor.read_prev_meta().unwrap();
		let item_3 = cursor.read_prev_meta().unwrap();
		let item_4 = cursor.read_prev_meta().unwrap();
		let item_5 = cursor.read_prev_meta().unwrap();

		// then
		assert_eq!(
			item_1,
			Some(WalItemMeta {
				kind: WalItemKind::Commit,
				transaction_id: 0,
				sequence_num: 3,
			})
		);
		assert_eq!(
			item_2,
			Some(WalItemMeta {
				kind: WalItemKind::Undo,
				transaction_id: 1,
				sequence_num: 2,
			})
		);
		assert_eq!(
			item_3,
			Some(WalItemMeta {
				kind: WalItemKind::Write,
				transaction_id: 1,
				sequence_num: 1,
			})
		);
		assert_eq!(
			item_4,
			Some(WalItemMeta {
				kind: WalItemKind::Write,
				transaction_id: 0,
				sequence_num: 0,
			})
		);
		assert_eq!(item_5, None);
	}
}
