use std::path::PathBuf;

use static_assertions::assert_impl_all;

pub(super) struct StorageDir {
	path: PathBuf,
}

assert_impl_all!(StorageDir: Send, Sync);

impl StorageDir {
	const META_FILE_NAME: &'static str = "storage.acnm";

	pub fn new(path: PathBuf) -> Self {
		Self { path }
	}

	pub fn segment_file(&self, segment_num: u32) -> PathBuf {
		self.path.join(format!("{segment_num}.acns"))
	}

	pub fn meta_file(&self) -> PathBuf {
		self.path.join(Self::META_FILE_NAME)
	}
}

#[cfg(test)]
mod tests {
	use std::fs;

	use tempfile::tempdir;

	use super::*;

	#[test]
	fn open_segment_file() {
		let dir = tempdir().unwrap();

		fs::write(dir.path().join("0.acns"), [69]).unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		let buf = fs::read(storage_dir.segment_file(0)).unwrap();
		assert_eq!(buf, vec![69]);
	}

	#[test]
	fn open_meta_file() {
		let dir = tempdir().unwrap();

		fs::write(dir.path().join("storage.acnm"), [69]).unwrap();

		let storage_dir = StorageDir::new(dir.path().into());
		let buf = fs::read(storage_dir.meta_file()).unwrap();
		assert_eq!(buf, vec![69]);
	}
}
