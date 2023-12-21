use std::{io, usize};

use thiserror::Error;

use super::file::StorageFile;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
    NotAStorageFile,

    #[error("The format version {0} is not supported in this version of acorn")]
    UnsupportedVersion(u8),

    #[error("The storage is corrupted (Unexpected end of file)")]
    UnexpectedEOF,

    #[error("Failed to expand storage file")]
    IncompleteWrite,

    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct Storage<F: StorageFile> {
    pub meta: Meta,
    file: F,
}

impl<F: StorageFile> Storage<F> {
    fn load(file: F) -> Result<Self, Error> {
        let meta = Meta::read_from(&file)?;
        todo!()
    }
}

const CURRENT_VERSION: u8 = 1;
const MAGIC: [u8; 4] = *b"ACRN";

/*
 * Metadata layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The magic bytes "ACRN" (hex: 0x41 0x43 0x52 0x4e)                          |
 * |      4 |    1 | The format version. Must be 1.                                             |
 * |      5 |    1 | The base 2 logarithm of the page size used in the file (big-endian).       |
 * |      6 |   26 | Reserved for future use. Must be zero.                                     |
 *
 */

#[derive(Debug, Clone)]
pub struct Meta {
    pub format_version: u8,
    pub page_size_exponent: u8,
    pub page_size: usize,
}

impl Meta {
    const SIZE: usize = 32;

    fn new(page_size_exponent: u8) -> Self {
        Self {
            format_version: CURRENT_VERSION,
            page_size_exponent,
            page_size: 1 << page_size_exponent,
        }
    }

    fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
        let mut buffer: [u8; Self::SIZE] = Default::default();
        let bytes_read = file.read_at(&mut buffer, 0)?;

        let magic: [u8; 4] = buffer[0..4].try_into().unwrap();
        if magic != MAGIC {
            return Err(Error::NotAStorageFile);
        }

        if bytes_read != buffer.len() {
            return Err(Error::UnexpectedEOF);
        }

        let format_version = buffer[4];
        if format_version != 1 {
            return Err(Error::UnsupportedVersion(format_version));
        }

        let page_size_exponent = u8::from_be_bytes(buffer[5..6].try_into().unwrap());

        Ok(Self {
            format_version,
            page_size_exponent,
            page_size: 1 << page_size_exponent,
        })
    }

    fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
        let mut buf: [u8; Self::SIZE] = Default::default();

        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = self.format_version;
        buf[5..6].copy_from_slice(&self.page_size_exponent.to_be_bytes());

        if file.write_at(&buf, 0)? != buf.len() {
            return Err(Error::IncompleteWrite);
        }
        Ok(())
    }
}

/*
 * State block layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The current size of the dababase in pages.                                 |
 * |      4 |    4 | The first page of the freelist.                                            |
 * |      8 |    4 | The current length of the freelist.                                        |
 * |     12 |   20 | Reserved for future use. Must be zero.                                     |
 *
 */

#[derive(Debug)]
struct State {
    num_pages: u32,
    freelist_trunk: u32,
    freelist_length: u32,
}

impl State {
    const SIZE: usize = 32;
    const OFFSET: u64 = Meta::SIZE as u64;

    fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
        let mut buf: [u8; Self::SIZE] = Default::default();
        if file.read_at(&mut buf, Self::OFFSET)? != buf.len() {
            return Err(Error::UnexpectedEOF);
        }

        let num_pages = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let freelist_trunk = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let freelist_length = u32::from_be_bytes(buf[8..12].try_into().unwrap());

        Ok(Self {
            num_pages,
            freelist_trunk,
            freelist_length,
        })
    }

    fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
        let mut buf: [u8; Self::SIZE] = Default::default();

        buf[0..4].copy_from_slice(&self.num_pages.to_be_bytes());
        buf[4..8].copy_from_slice(&self.freelist_trunk.to_be_bytes());
        buf[8..12].copy_from_slice(&self.freelist_length.to_be_bytes());

        if file.write_at(&buf, Self::OFFSET)? != buf.len() {
            return Err(Error::IncompleteWrite);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn read_metadata() {
        let data = vec![
            0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let meta = Meta::read_from(&data).unwrap();
        assert_eq!(meta.format_version, 1);
        assert_eq!(meta.page_size, 1024);
    }

    #[test]
    fn try_read_with_invalid_magic() {
        let data = vec![
            0x42, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_matches!(Meta::read_from(&data), Err(Error::NotAStorageFile));
    }

    #[test]
    fn try_read_with_unsupported_version() {
        let data = vec![
            0x41, 0x43, 0x52, 0x4e, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        assert_matches!(Meta::read_from(&data), Err(Error::UnsupportedVersion(..)));
    }

    #[test]
    fn try_read_incomplete_magic() {
        let data = vec![0x41, 0x43];

        assert_matches!(Meta::read_from(&data), Err(Error::NotAStorageFile));
    }

    #[test]
    fn try_read_incomplete_meta() {
        let data = vec![0x41, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00];

        assert_matches!(Meta::read_from(&data), Err(Error::UnexpectedEOF));
    }

    #[test]
    fn write_metadata() {
        let mut data = Vec::new();

        let meta = Meta {
            format_version: 1,
            page_size_exponent: 10,
            page_size: 1024,
        };

        meta.write_to(&mut data).unwrap();

        assert_eq!(
            data,
            vec![
                0x41, 0x43, 0x52, 0x4e, 0x01, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00
            ]
        )
    }

    #[test]
    fn read_state() {
        let data = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x45, 0x00, 0x00, 0x01, 0xa4, 0x00, 0x00,
            0xa4, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let state = State::read_from(&data).unwrap();

        assert_eq!(state.num_pages, 69);
        assert_eq!(state.freelist_trunk, 420);
        assert_eq!(state.freelist_length, 42069);
    }

    #[test]
    fn try_read_incomplete_state() {
        let data = vec![0x00, 0x00, 0x00];

        assert_matches!(State::read_from(&data), Err(Error::UnexpectedEOF));
    }

    #[test]
    fn write_state() {
        let mut data = Vec::<u8>::new();

        let state = State {
            num_pages: 123,
            freelist_trunk: 543,
            freelist_length: 5432,
        };
        state.write_to(&mut data).unwrap();

        assert_eq!(
            data,
            vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x00, 0x02, 0x1f, 0x00, 0x00,
                0x15, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
            ]
        );
    }
}
