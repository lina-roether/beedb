use std::{
	io::{Read, Seek, SeekFrom, Write},
	mem::size_of,
};

use musli_zerocopy::{OwnedBuf, Ref, ZeroCopy};

use super::{FileError, FileType, GenericHeader, GenericHeaderInit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ZeroCopy)]
#[repr(u8)]
enum ItemType {
	Write = 0,
	Commit = 1,
	Undo = 2,
}

#[derive(Debug, ZeroCopy)]
#[repr(C)]
struct ItemHeader {
	item_type: ItemType,
	flags: u8,
	data_length: u16,
	crc: u32,
	transaction_id: u64,
	sequence_num: u64,
}

#[derive(Debug, ZeroCopy)]
#[repr(C)]
struct ItemFooter {
	start_offset: i16,
}

pub(crate) struct WalFile<F: Seek + Read + Write> {
	body_start: u64,
	file: F,
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
		})
	}

	fn open(mut file: F) -> Result<Self, FileError> {
		file.seek(SeekFrom::Start(0))?;
		let mut header_buf =
			OwnedBuf::with_capacity_and_alignment::<GenericHeader>(size_of::<GenericHeader>());
		file.read_exact(header_buf.as_mut_slice())?;

		let header: &GenericHeader = header_buf.load_at(0)?;

		header.validate()?;
		if header.file_type != FileType::Wal {
			return Err(FileError::WrongFileType(header.file_type));
		}
		Ok(Self {
			body_start: header.content_offset.into(),
			file,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::io::Cursor;

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
}
