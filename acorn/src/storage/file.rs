use std::{fs::File, io, iter, ops::Range, usize};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub trait StorageFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize>;
}

fn get_buf_range(len: usize, buf_len: usize, offset: u64) -> Range<usize> {
    if offset >= len as u64 {
        return 0..0;
    };
    let start = offset as usize;
    start..usize::min(start + buf_len, len)
}

impl StorageFile for Vec<u8> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let range = get_buf_range(self.len(), buf.len(), offset);
        let num_read = range.len();
        buf[0..num_read].copy_from_slice(&self[range]);
        Ok(num_read)
    }

    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let min_length = (offset as usize) + buf.len();
        if self.len() < min_length {
            self.resize(min_length, 0);
        }
        let range = get_buf_range(self.len(), buf.len(), offset);
        let num_written = range.len();
        self[range].copy_from_slice(&buf[0..num_written]);
        Ok(num_written)
    }
}

cfg_match! {
    cfg(unix) => {
        impl StorageFile for File {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                FileExt::read_at(self, buf, offset)
            }

            fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
                FileExt::write_at(self, buf, offset)
            }
        }
    }
    cfg(windows) => {
        impl StorageFile for File {
            fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
                FileExt::seek_read(self, buf, offset)
            }

            fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
                FileExt::seek_write(self, buf, offset)
            }
        }
    }
    _ => {
        compile_error!("The Acorn storage engine is not supported on this platform");
    }
}
