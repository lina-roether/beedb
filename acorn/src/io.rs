use std::{fs::File, io, ops::Range};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

use crate::utils::aligned_buf::AlignedBuffer;

pub trait IoTarget {
	fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

	fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize>;

	fn set_len(&mut self, len: u64) -> io::Result<()>;
}

fn get_buf_range(len: usize, buf_len: usize, offset: u64) -> Range<usize> {
	if offset >= len as u64 {
		return 0..0;
	};
	let start = offset as usize;
	start..usize::min(start + buf_len, len)
}

impl IoTarget for AlignedBuffer {
	fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
		let range = get_buf_range(self.len(), buf.len(), offset);
		let num_read = range.len();
		buf[0..num_read].copy_from_slice(&self[range]);
		Ok(num_read)
	}

	fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
		let min_length = (offset as usize) + buf.len();
		if self.len() < min_length {
			self.resize_to(min_length);
		}
		let range = get_buf_range(self.len(), buf.len(), offset);
		let num_written = range.len();
		self[range].copy_from_slice(&buf[0..num_written]);
		Ok(num_written)
	}

	fn set_len(&mut self, len: u64) -> io::Result<()> {
		self.resize_to(len as usize);
		Ok(())
	}
}

cfg_match! {
	cfg(unix) => {
		impl IoTarget for File {
			fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
				FileExt::read_at(self, buf, offset)
			}

			fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
				FileExt::write_at(self, buf, offset)
			}

			fn set_len(&mut self, len: u64) -> io::Result<()> {
				File::set_len(self, len)
			}
		}
	}
	cfg(windows) => {
		impl IoTarget for File {
			fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
				FileExt::seek_read(self, buf, offset)
			}

			fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<usize> {
				FileExt::seek_write(self, buf, offset)
			}

			fn set_len(&mut self, len: u64) -> io::Result<()> {
				File::set_len(self, len)
			}
		}
	}
	_ => {
		compile_error!("The Acorn storage engine is not supported on this platform");
	}
}
