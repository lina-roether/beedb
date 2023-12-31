use std::{
	fs::{File, OpenOptions},
	io,
	path::PathBuf,
};

use static_assertions::assert_impl_all;

pub struct StorageDir {
	path: PathBuf,
}

assert_impl_all!(StorageDir: Send, Sync);

impl StorageDir {
	const META_FILE_NAME: &'static str = "storage.acnm";

	pub fn new(path: PathBuf) -> Self {
		Self { path }
	}

	pub fn open_segment_file(&self, segment_num: u32, create: bool) -> Result<File, io::Error> {
		OpenOptions::new()
			.read(true)
			.write(true)
			.create(create)
			.open(self.path.join(Self::segment_file_name(segment_num)))
	}

	pub fn open_meta_file(&self, create: bool) -> Result<File, io::Error> {
		OpenOptions::new()
			.read(true)
			.write(true)
			.create(create)
			.open(self.path.join(Self::META_FILE_NAME))
	}

	#[inline]
	fn segment_file_name(segment_num: u32) -> String {
		format!("{segment_num}.acns")
	}
}

#[cfg(test)]
mod tests {
	use std::{fs, io::Read};

	use tempfile::tempdir;

	use super::*;

	#[test]
	fn open_segment_file() {
		let dir = tempdir().unwrap();

		fs::write(dir.path().join("0.acns"), [69]).unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		let mut buf = Vec::new();
		storage_dir
			.open_segment_file(0, false)
			.unwrap()
			.read_to_end(&mut buf)
			.unwrap();
		assert_eq!(buf, vec![69]);
	}

	#[test]
	fn open_meta_file() {
		let dir = tempdir().unwrap();

		fs::write(dir.path().join("storage.acnm"), [69]).unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		let mut buf = Vec::new();
		storage_dir
			.open_meta_file(false)
			.unwrap()
			.read_to_end(&mut buf)
			.unwrap();
		assert_eq!(buf, vec![69]);
	}

	#[test]
	fn create_segment_file() {
		let dir = tempdir().unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		storage_dir.open_segment_file(1, true).unwrap();

		assert!(dir.path().join("1.acns").exists());
	}

	#[test]
	fn create_meta_file() {
		let dir = tempdir().unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		storage_dir.open_meta_file(true).unwrap();

		assert!(dir.path().join("storage.acnm").exists());
	}

	#[test]
	fn dont_create_segment_file_when_flag_is_not_set() {
		let dir = tempdir().unwrap();
		let storage_dir = StorageDir::new(dir.path().into());
		assert!(storage_dir.open_segment_file(69, false).is_err());
		assert!(!dir.path().join("69.acns").exists());
	}

	#[test]
	fn dont_create_meta_file_when_flag_is_not_set() {
		let dir = tempdir().unwrap();
		let storage_dir = StorageDir::new(dir.path().into());
		assert!(storage_dir.open_meta_file(false).is_err());
		assert!(!dir.path().join("storage.acnm").exists());
	}
}
