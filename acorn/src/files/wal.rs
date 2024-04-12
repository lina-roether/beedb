use std::{
	borrow::Cow,
	io::{Read, Seek, SeekFrom, Write},
	mem::size_of,
};

use mockall::automock;
use musli_zerocopy::{OwnedBuf, ZeroCopy};

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

#[derive(Debug)]
pub(crate) struct WalWriteItemData<'a> {
	pub segment_id: u32,
	pub page_id: u16,
	pub offset: u16,
	pub data: Cow<'a, [u8]>,
}

#[derive(Debug)]
pub(crate) enum WalItemData<'a> {
	Write(WalWriteItemData<'a>),
	Commit,
	Undo,
}

#[derive(Debug)]
pub(crate) struct WalItem<'a> {
	pub transaction_id: u64,
	pub sequence_num: u64,
	pub data: WalItemData<'a>,
}

#[derive(Debug)]
pub(crate) struct WalItemMeta {
	pub transaction_id: u64,
	pub sequence_num: u64,
	pub kind: WalItemKind,
}

#[automock]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait WalFileApi {
	fn push_item<'a>(&mut self, item: WalItem<'a>) -> Result<(), FileError>;
}

impl<F: Seek + Read + Write> WalFileApi for WalFile<F> {
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
}

#[cfg(test)]
mod tests {
	use std::{io::Cursor, mem::size_of};

	use super::*;

	#[test]
	fn create_wal_file() {
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		WalFile::create(&mut file).unwrap();

		let mut buf = OwnedBuf::new();
		buf.extend_from_slice(&file.into_inner());
		let header: &GenericHeader = buf.load_at(0).unwrap();
		assert_eq!(header.file_type, FileType::Wal);
		assert_eq!(header.content_offset, size_of::<GenericHeader>() as u16);
	}

	#[test]
	fn open_wal_file() {
		let mut buf = OwnedBuf::new();
		buf.store(&GenericHeader::new(GenericHeaderInit {
			file_type: FileType::Wal,
			header_size: 0,
		}));

		let file = Cursor::new(Vec::from(buf.as_mut_slice()));
		WalFile::open(file).unwrap();
	}

	#[test]
	fn push_wal_write_item() {
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		let mut wal = WalFile::create(&mut file).unwrap();
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

		let mut buf = OwnedBuf::new();
		buf.extend_from_slice(&file.into_inner());

		let item_header: &ItemHeader = buf.load_at(size_of::<GenericHeader>()).unwrap();
		assert_eq!(item_header.kind, WalItemKind::Write);
		assert_eq!(item_header.item_length, 42);
		assert_eq!(item_header.transaction_id, 69);
		assert_eq!(item_header.sequence_num, 420);

		let write_item_header: &WriteItemHeader = buf
			.load_at(size_of::<GenericHeader>() + size_of::<ItemHeader>())
			.unwrap();
		assert_eq!(write_item_header.segment_id, 25);
		assert_eq!(write_item_header.page_id, 24);
		assert_eq!(write_item_header.offset, 12);
		assert_eq!(write_item_header.crc, 0x3991);

		let data: &[u8; 4] = buf
			.load_at(
				size_of::<GenericHeader>() + size_of::<ItemHeader>() + size_of::<WriteItemHeader>(),
			)
			.unwrap();
		assert_eq!(data, &[1, 2, 3, 4]);

		let item_footer: &ItemFooter = buf
			.load_at(
				size_of::<GenericHeader>()
					+ size_of::<ItemHeader>()
					+ size_of::<WriteItemHeader>()
					+ 4,
			)
			.unwrap();
		assert_eq!(item_footer.item_length, 42);
	}

	#[test]
	fn push_wal_commit_item() {
		let mut file: Cursor<Vec<u8>> = Cursor::new(Vec::new());
		let mut wal = WalFile::create(&mut file).unwrap();
		wal.push_item(WalItem {
			transaction_id: 69,
			sequence_num: 420,
			data: WalItemData::Commit,
		})
		.unwrap();

		let mut buf = OwnedBuf::new();
		buf.extend_from_slice(&file.into_inner());

		let item_header: &ItemHeader = buf.load_at(size_of::<GenericHeader>()).unwrap();
		assert_eq!(item_header.kind, WalItemKind::Commit);
		assert_eq!(item_header.item_length, 26);
		assert_eq!(item_header.transaction_id, 69);
		assert_eq!(item_header.sequence_num, 420);

		let item_footer: &ItemFooter = buf
			.load_at(size_of::<GenericHeader>() + size_of::<ItemHeader>())
			.unwrap();
		assert_eq!(item_footer.item_length, 26);
	}
}
