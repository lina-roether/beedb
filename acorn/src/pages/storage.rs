use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    mem::size_of,
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("Provided file is not an acorn storage file")]
    Magic,

    #[error("Provided file uses an outdated format and is not supported (format version: {0})")]
    OutdatedVersion(u32),

    #[error("Provided file is not supported by this version of acorn (format version: {0})")]
    FutureVersion(u32),

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum AccessError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct PageStorage<F>
where
    F: Read + Write + Seek,
{
    page_size: usize,
    content_offset: usize,
    file: F,
}

impl<F> PageStorage<F>
where
    F: Read + Write + Seek,
{
    const MAGIC: u32 = u32::from_le_bytes(*b"TOME");
    const FORMAT_VERSION: u32 = 1;

    pub fn init(mut file: F, page_size: usize) -> Result<Self, AccessError> {
        let header = StorageFileHeader {
            magic: Self::MAGIC,
            format_version: Self::FORMAT_VERSION,
            page_size: page_size as u32,
            content_offset: size_of::<StorageFileHeader>() as u32,
        };
        header.write(&mut file)?;

        Ok(Self::from_header_and_file(header, file))
    }

    pub fn load(mut file: F) -> Result<Self, LoadError> {
        let header = StorageFileHeader::read(&mut file)?;
        if header.magic != Self::MAGIC {
            return Err(LoadError::Magic);
        }
        if header.format_version < Self::FORMAT_VERSION {
            return Err(LoadError::OutdatedVersion(header.format_version));
        }
        if header.format_version > Self::FORMAT_VERSION {
            return Err(LoadError::FutureVersion(header.format_version));
        }

        Ok(Self::from_header_and_file(header, file))
    }

    pub fn read_page(&mut self, id: usize, buf: &mut [u8]) -> Result<(), AccessError> {
        debug_assert!(buf.len() == self.page_size);
        self.seek_to_page(id)?;
        self.file.read_exact(&mut buf[0..self.page_size])?;
        Ok(())
    }

    pub fn write_page(&mut self, id: usize, data: &[u8]) -> Result<(), AccessError> {
        debug_assert!(data.len() == self.page_size);
        self.seek_to_page(id)?;
        self.file.write_all(data)?;
        Ok(())
    }

    fn seek_to_page(&mut self, id: usize) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(
            (self.content_offset + id * self.page_size) as u64,
        ))?;
        Ok(())
    }

    fn from_header_and_file(header: StorageFileHeader, file: F) -> Self {
        Self {
            page_size: header.page_size as usize,
            content_offset: header.content_offset as usize,
            file,
        }
    }
}

#[derive(Debug, Clone)]
struct StorageFileHeader {
    magic: u32,
    format_version: u32,
    page_size: u32,
    content_offset: u32,
}

impl StorageFileHeader {
    fn read<F>(f: &mut F) -> Result<Self, io::Error>
    where
        F: Read + Seek,
    {
        let mut buffer = [0; 4];

        f.seek(SeekFrom::Start(0))?;

        f.read_exact(&mut buffer)?;
        let magic = u32::from_le_bytes(buffer);

        f.read_exact(&mut buffer)?;
        let format_version = u32::from_le_bytes(buffer);

        f.read_exact(&mut buffer)?;
        let page_size = u32::from_le_bytes(buffer);

        f.read_exact(&mut buffer)?;
        let content_offset = u32::from_le_bytes(buffer);

        Ok(Self {
            magic,
            format_version,
            page_size,
            content_offset,
        })
    }

    fn write<F>(&self, f: &mut F) -> Result<(), io::Error>
    where
        F: Write + Seek,
    {
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&self.magic.to_le_bytes())?;
        f.write_all(&self.format_version.to_le_bytes())?;
        f.write_all(&self.page_size.to_le_bytes())?;
        f.write_all(&self.content_offset.to_le_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn file_init() {
        let storage = PageStorage::init(Cursor::new(Vec::new()), 1024).unwrap();

        #[rustfmt::skip]
        assert_eq!(
            storage.file.into_inner(),
            vec![
                0x54, 0x4f, 0x4d, 0x45,
                0x01, 0x00, 0x00, 0x00,
                0x00, 0x04, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00
            ]
        );
    }

    #[test]
    fn file_load() {
        #[rustfmt::skip]
        let file_data = [
            0x54, 0x4f, 0x4d, 0x45,
            0x01, 0x00, 0x00, 0x00,
            0x00, 0x08, 0x00, 0x00,
            0x10, 0x00, 0x00, 0x00
        ];

        let storage = PageStorage::load(Cursor::new(file_data)).unwrap();
        assert_eq!(storage.page_size, 2048);
        assert_eq!(storage.content_offset, 16);
    }

    #[test]
    fn page_read_and_write() {
        let page_data_1 = [0x12, 0x23, 0x45, 0x56];
        let page_data_2 = [0x12, 0x23, 0x45, 0x56];

        let mut storage = PageStorage::init(Cursor::new(Vec::new()), 4).unwrap();
        storage.write_page(69, &page_data_1).unwrap();
        storage.write_page(420, &page_data_2).unwrap();

        let mut result_1 = [0; 4];
        storage.read_page(69, &mut result_1).unwrap();

        let mut result_2 = [0; 4];
        storage.read_page(420, &mut result_2).unwrap();

        assert_eq!(result_1, page_data_1);
        assert_eq!(result_2, page_data_2);
    }
}
