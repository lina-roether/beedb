use std::io;

use thiserror::Error;

use self::file::StorageFile;

mod file;

#[derive(Debug, Error)]
pub enum Error {
    #[error("The provided file is not an acorn storage file (expected magic bytes {MAGIC:08x?})")]
    Magic,

    #[error("The format version {0} is not supported in this version of acorn")]
    UnsupportedVersion(u8),

    #[error("The storage is corrupted")]
    CorruptedMeta,

    #[error(transparent)]
    Io(#[from] io::Error),
}

const MAGIC: [u8; 4] = *b"ACRN";

/*
 * Metadata layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The magic bytes "ACRN" (hex: 0x41 0x43 0x52 0x4e)                          |
 * |      4 |    1 | The format version. Must be 1.                                             |
 * |      5 |    4 | The page size used in the file.                                            |
 * |      9 |   23 | Reserved for future use. Must be zero.                                     |
 *
 */

struct Meta {
    format_version: u8,
    page_size: usize,
}

impl Meta {
    fn read_from(file: impl StorageFile) -> Result<Self, Error> {
        let mut buffer: [u8; 32] = Default::default();
        let bytes_read = file.read_at(&mut buffer, 0)?;

        let magic: [u8; 4] = buffer[0..4].try_into().unwrap();
        if magic != MAGIC {
            return Err(Error::Magic);
        }

        if bytes_read != buffer.len() {
            return Err(Error::CorruptedMeta);
        }

        let format_version = buffer[4];
        if format_version != 1 {
            return Err(Error::UnsupportedVersion(format_version));
        }

        let page_size = u32::from_le_bytes(buffer[5..9].try_into().unwrap()) as usize;

        Ok(Self {
            format_version,
            page_size,
        })
    }
}
