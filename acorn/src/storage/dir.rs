use std::{
	fs::{File, OpenOptions},
	io,
	path::PathBuf,
};

pub struct StorageDir {
	path: PathBuf,
}

impl StorageDir {
	const META_FILE_NAME: &'static str = "storage.acnm";

	pub fn new(path: PathBuf) -> Self {
		Self { path }
	}

	pub fn open_segment_file(&self, segment_num: u32) -> Result<File, io::Error> {
		OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(self.path.join(Self::segment_file_name(segment_num)))
	}

	pub fn open_meta(&self) -> Result<File, io::Error> {
		OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
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
			.open_segment_file(0)
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
			.open_meta()
			.unwrap()
			.read_to_end(&mut buf)
			.unwrap();
		assert_eq!(buf, vec![69]);
	}

	#[test]
	fn create_segment_file() {
		let dir = tempdir().unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		storage_dir.open_segment_file(1).unwrap();

		assert!(dir.path().join("1.acns").exists());
	}

	#[test]
	fn create_meta_file() {
		let dir = tempdir().unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		storage_dir.open_meta().unwrap();

		assert!(dir.path().join("storage.acnm").exists());
	}
}
