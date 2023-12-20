use std::{io, usize};

use thiserror::Error;

use super::file::StorageFile;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
    NotAStorageFile,

    #[error("The format version {0} is not supported in this version of acorn")]
    UnsupportedVersion(u8),

    #[error("The storage is corrupted")]
    CorruptedMeta,

    #[error("Failed to expand storage file")]
    IncompleteWrite,

    #[error(transparent)]
    Io(#[from] io::Error),
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
 * |      6 |   10 | Reserved for future use. Must be zero.                                     |
 *
 */

#[derive(Debug, Clone)]
struct Meta {
    pub format_version: u8,
    pub page_size_exponent: usize,
    pub page_size: usize,
}

impl Meta {
    fn new(page_size_exponent: usize) -> Self {
        Self {
            format_version: CURRENT_VERSION,
            page_size_exponent,
            page_size: 1 << page_size_exponent,
        }
    }

    fn read_from(file: &impl StorageFile) -> Result<Self, Error> {
        let mut buffer: [u8; 32] = Default::default();
        let bytes_read = file.read_at(&mut buffer, 0)?;

        let magic: [u8; 4] = buffer[0..4].try_into().unwrap();
        if magic != MAGIC {
            return Err(Error::NotAStorageFile);
        }

        if bytes_read != buffer.len() {
            return Err(Error::CorruptedMeta);
        }

        let format_version = buffer[4];
        if format_version != 1 {
            return Err(Error::UnsupportedVersion(format_version));
        }

        let page_size_exponent = u32::from_be_bytes(buffer[5..9].try_into().unwrap()) as usize;

        Ok(Self {
            format_version,
            page_size_exponent,
            page_size: 1 << page_size_exponent,
        })
    }

    fn write_to(&self, file: &mut impl StorageFile) -> Result<(), Error> {
        let mut buf: [u8; 32] = Default::default();

        buf[0..4].copy_from_slice(&MAGIC);
        buf[5] = self.format_version;
        buf[5..9].copy_from_slice(&self.page_size.to_be_bytes());

        if file.write_at(&buf, 0)? != buf.len() {
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
            0x41, 0x43, 0x52, 0x4e, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
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

        assert_matches!(Meta::read_from(&data), Err(Error::CorruptedMeta));
    }
}
