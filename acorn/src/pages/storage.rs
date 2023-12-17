use std::{
    assert_matches::assert_matches,
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
pub enum InitError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("Page {0} is empty")]
    Empty(usize),

    #[error("Page {0} doesn't exist")]
    Nonexistent(usize),

    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum AllocationError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum FreeError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct PageStorage<F>
where
    F: Read + Write + Seek,
{
    page_size: usize,
    page_size_with_header: usize,
    content_offset: usize,
    buffer: Box<[u8]>,
    next_free: usize,
    file: F,
}

impl<F> PageStorage<F>
where
    F: Read + Write + Seek,
{
    const MAGIC: u32 = u32::from_le_bytes(*b"TOME");
    const FORMAT_VERSION: u32 = 1;
    const HEADER_SIZE: usize = 1;
    const MIN_SIZE: usize = size_of::<u64>();

    pub fn init(mut file: F, page_size: usize) -> Result<Self, InitError> {
        debug_assert!(page_size >= Self::MIN_SIZE);
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

    pub fn read_page(&mut self, id: usize, buf: &mut [u8]) -> Result<(), ReadError> {
        debug_assert!(buf.len() == self.page_size);
        match self.read_page_raw(id)? {
            None => return Err(ReadError::Nonexistent(id)),
            Some(StoredPage::Free { .. }) => return Err(ReadError::Empty(id)),
            Some(StoredPage::Occupied(data)) => buf.copy_from_slice(data),
        }
        Ok(())
    }

    pub fn write_page(&mut self, id: usize, data: &[u8]) -> Result<(), WriteError> {
        debug_assert!(data.len() == self.page_size);
        #[cfg(debug_assertions)]
        {
            assert_matches!(self.read_page_raw(id), Ok(Some(StoredPage::Occupied(..))));
        }

        self.buffer[0] = true as u8;
        self.buffer[1..].copy_from_slice(data);

        self.seek_to_page(id)?;
        self.write_buffer()?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<usize, AllocationError> {
        let new_next_free = match self.read_page_raw(self.next_free)? {
            Some(new_page) => {
                let StoredPage::Free { next } = new_page else {
                    unreachable!();
                };
                next
            }
            None => self.next_free + 1,
        };

        let allocated = self.next_free;
        self.set_allocated(allocated)?;
        self.next_free = new_next_free;
        Ok(allocated)
    }

    pub fn free_page(&mut self, id: usize) -> Result<(), FreeError> {
        self.set_free(id, self.next_free)?;
        self.next_free = id;
        Ok(())
    }

    fn read_page_raw(&mut self, id: usize) -> Result<Option<StoredPage>, io::Error> {
        self.seek_to_page(id)?;
        let bytes_read = self.file.read(&mut self.buffer)?;
        if bytes_read != self.buffer.len() {
            return Ok(None);
        }
        let occupied = self.buffer[0] != 0;
        if occupied {
            return Ok(Some(StoredPage::Occupied(&self.buffer[1..])));
        }

        let next = u64::from_le_bytes(self.buffer[1..9].try_into().unwrap());
        Ok(Some(StoredPage::Free {
            next: next as usize,
        }))
    }

    fn set_allocated(&mut self, id: usize) -> Result<(), io::Error> {
        self.clear_buffer();
        self.buffer[0] = true as u8;
        self.seek_to_page(id)?;
        self.write_buffer()?;
        Ok(())
    }

    fn set_free(&mut self, id: usize, next_free: usize) -> Result<(), io::Error> {
        self.clear_buffer();
        self.buffer[0] = false as u8;
        self.buffer[1..9].copy_from_slice(&next_free.to_le_bytes());
        self.seek_to_page(id)?;
        self.write_buffer()?;
        Ok(())
    }

    #[inline]
    fn clear_buffer(&mut self) {
        self.buffer.fill(0);
    }

    #[inline]
    fn read_to_buffer(&mut self) -> Result<(), io::Error> {
        self.file.read_exact(&mut self.buffer)
    }

    #[inline]
    fn write_buffer(&mut self) -> Result<(), io::Error> {
        self.file.write_all(&self.buffer)
    }

    fn seek_to_page(&mut self, id: usize) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(
            (self.content_offset + id * self.page_size_with_header) as u64,
        ))?;
        Ok(())
    }

    fn from_header_and_file(header: StorageFileHeader, file: F) -> Self {
        let page_size = header.page_size as usize;
        let page_size_with_header = page_size + Self::HEADER_SIZE;
        Self {
            page_size,
            page_size_with_header,
            content_offset: header.content_offset as usize,
            file,
            next_free: 0,
            buffer: vec![0_u8; page_size_with_header].into_boxed_slice(),
        }
    }
}

#[derive(Debug)]
enum StoredPage<'a> {
    Free { next: usize },
    Occupied(&'a [u8]),
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
    use std::{assert_matches::assert_matches, io::Cursor};

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
    fn page_allocate_read_and_write() {
        let page_data_1 = [0x12, 0x23, 0x45, 0x56, 0x45, 0x43, 0x12, 0x89];
        let page_data_2 = [0x82, 0x23, 0x25, 0x56, 0x78, 0x12, 0x43, 0x48];

        let mut storage = PageStorage::init(Cursor::new(Vec::new()), 8).unwrap();

        let page_1 = storage.allocate_page().unwrap();
        let page_2 = storage.allocate_page().unwrap();

        storage.write_page(page_1, &page_data_1).unwrap();
        storage.write_page(page_2, &page_data_2).unwrap();

        let mut result_1 = [0; 8];
        storage.read_page(page_1, &mut result_1).unwrap();

        let mut result_2 = [0; 8];
        storage.read_page(page_2, &mut result_2).unwrap();

        assert_eq!(result_1, page_data_1);
        assert_eq!(result_2, page_data_2);
    }

    #[test]
    fn try_read_nonexistent_page() {
        let mut storage = PageStorage::init(Cursor::new(Vec::new()), 8).unwrap();

        assert_matches!(
            storage.read_page(69, &mut [0; 8]),
            Err(ReadError::Nonexistent(69))
        );
    }

    #[test]
    fn try_read_freed_page() {
        let mut storage = PageStorage::init(Cursor::new(Vec::new()), 8).unwrap();

        let page = storage.allocate_page().unwrap();
        storage.free_page(page).unwrap();

        assert_matches!(
            storage.read_page(page, &mut [0; 8]),
            Err(ReadError::Empty(..))
        )
    }

    #[test]
    fn page_allocation_pattern() {
        let mut storage = PageStorage::init(Cursor::new(Vec::new()), 8).unwrap();

        let page_1 = storage.allocate_page().unwrap();
        let page_2 = storage.allocate_page().unwrap();

        storage.free_page(page_1).unwrap();
        storage.free_page(page_2).unwrap();

        let page_3 = storage.allocate_page().unwrap();
        let page_4 = storage.allocate_page().unwrap();
        let page_5 = storage.allocate_page().unwrap();

        assert_eq!(page_1, 0);
        assert_eq!(page_2, 1);
        assert_eq!(page_3, 1);
        assert_eq!(page_4, 0);
        assert_eq!(page_5, 2);
    }
}
