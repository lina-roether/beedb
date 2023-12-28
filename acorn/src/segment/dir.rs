use std::{
	fs::{File, OpenOptions},
	path::PathBuf,
};

use crate::{
	storage::{Storage, StorageError},
	utils::byte_view::ByteView,
};

pub struct DataDir {
	path: PathBuf,
}

impl DataDir {
	const META_FILE_NAME: &'static str = "ameta";

	pub fn new(path: PathBuf) -> Self {
		Self { path }
	}

	pub fn open_cluster(&self, cluster_num: u32) -> Result<Storage<File>, StorageError> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.open(self.path.join(Self::cluster_name(cluster_num)))?;
		Storage::load(file)
	}

	#[inline]
	fn cluster_name(cluster_num: u32) -> String {
		format!("{cluster_num}.acsg")
	}
}

const META_MAGIC: [u8; 4] = *b"ASGM";
const META_FORMAT_VERSION: u8 = 1;

struct MetaFile {
	magic: [u8; 4],
	format_version: u8,
	byte_order: u8,
	num_clusters: u32,
}

unsafe impl ByteView for MetaFile {}
