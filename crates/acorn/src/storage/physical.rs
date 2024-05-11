use std::{
	collections::{HashMap, VecDeque},
	mem,
	sync::Arc,
};

#[cfg(test)]
use mockall::automock;

use parking_lot::RwLock;
use static_assertions::assert_impl_all;

use crate::{
	consts::DEFAULT_MAX_NUM_OPEN_SEGMENTS,
	files::{segment::SegmentFileApi, DatabaseFolder, DatabaseFolderApi},
};

use super::{PageId, StorageError, WalIndex};

pub(super) struct PhysicalStorage<DF = DatabaseFolder>
where
	DF: DatabaseFolderApi,
{
	folder: Arc<DF>,
	descriptor_cache: RwLock<DescriptorCache<DF>>,
}

assert_impl_all!(PhysicalStorage: Send, Sync);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalStorageConfig {
	pub max_num_open_segments: usize,
}

impl Default for PhysicalStorageConfig {
	fn default() -> Self {
		Self {
			max_num_open_segments: DEFAULT_MAX_NUM_OPEN_SEGMENTS,
		}
	}
}

impl<DF> PhysicalStorage<DF>
where
	DF: DatabaseFolderApi,
{
	fn new(folder: Arc<DF>, config: &PhysicalStorageConfig) -> Self {
		let descriptor_cache = RwLock::new(DescriptorCache::new(config));
		Self {
			folder,
			descriptor_cache,
		}
	}

	fn use_segment<T>(
		&self,
		segment_num: u32,
		handler: impl FnOnce(&DF::SegmentFile) -> Result<T, StorageError>,
	) -> Result<T, StorageError> {
		let cache = self.descriptor_cache.read();
		if let Some(segment) = cache.get_descriptor(segment_num) {
			return handler(segment);
		}
		mem::drop(cache);

		let mut cache = self.descriptor_cache.write();
		let segment_file = self.folder.open_segment_file(segment_num)?;
		let segment_file = cache.store_descriptor(segment_num, segment_file);
		handler(segment_file)
	}
}

#[cfg_attr(test, automock)]
pub(super) trait PhysicalStorageApi {
	fn read(&self, page_id: PageId, buf: &mut [u8]) -> Result<WalIndex, StorageError>;

	fn write(&self, page_id: PageId, buf: &[u8], wal_index: WalIndex) -> Result<(), StorageError>;
}

impl<DF: DatabaseFolderApi> PhysicalStorageApi for PhysicalStorage<DF> {
	fn read(&self, page_id: PageId, buf: &mut [u8]) -> Result<WalIndex, StorageError> {
		self.use_segment(page_id.segment_num, |segment| {
			let wal_index = segment.read(page_id.page_num, buf)?;
			Ok(wal_index)
		})
	}

	fn write(&self, page_id: PageId, buf: &[u8], wal_index: WalIndex) -> Result<(), StorageError> {
		self.use_segment(page_id.segment_num, |segment| {
			segment.write(page_id.page_num, buf, wal_index)?;
			Ok(())
		})
	}
}

struct DescriptorCache<DF: DatabaseFolderApi> {
	descriptors: HashMap<u32, DF::SegmentFile>,
	eviction_queue: VecDeque<u32>,
	max_num_open_segments: usize,
}

impl<DF: DatabaseFolderApi> DescriptorCache<DF> {
	fn new(config: &PhysicalStorageConfig) -> Self {
		let open_segments = HashMap::with_capacity(config.max_num_open_segments);
		let eviction_queue = VecDeque::with_capacity(config.max_num_open_segments);
		Self {
			descriptors: open_segments,
			eviction_queue,
			max_num_open_segments: config.max_num_open_segments,
		}
	}

	pub fn get_descriptor(&self, segment_num: u32) -> Option<&DF::SegmentFile> {
		self.descriptors.get(&segment_num)
	}

	pub fn store_descriptor(
		&mut self,
		segment_num: u32,
		segment_file: DF::SegmentFile,
	) -> &DF::SegmentFile {
		while self.descriptors.len() >= self.max_num_open_segments {
			self.evict_descriptor();
		}

		self.descriptors.insert(segment_num, segment_file);
		self.eviction_queue.push_back(segment_num);
		self.descriptors.get(&segment_num).unwrap()
	}

	// TODO: this can be optimized to use a more efficient cache eviction algorithm
	// in the future
	fn evict_descriptor(&mut self) {
		let Some(segment_num) = self.eviction_queue.pop_front() else {
			return;
		};
		self.descriptors.remove(&segment_num);
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		files::{
			segment::{MockSegmentFileApi, PAGE_BODY_SIZE},
			MockDatabaseFolderApi,
		},
		storage::test_helpers::{page_id, wal_index},
		utils::macros::non_zero,
	};
	use mockall::predicate::*;

	use super::*;

	#[test]
	fn write_to_storage() {
		// expect
		let mut folder = MockDatabaseFolderApi::new();
		folder
			.expect_open_segment_file()
			.once()
			.with(eq(69))
			.returning(|_| {
				let mut segment = MockSegmentFileApi::new();
				segment
					.expect_write()
					.once()
					.with(
						eq(non_zero!(420)),
						eq([1; PAGE_BODY_SIZE]),
						eq(wal_index!(69, 420)),
					)
					.returning(|_, _, _| Ok(()));
				Ok(segment)
			});

		// given
		let storage = PhysicalStorage::new(Arc::new(folder), &Default::default());

		// when
		storage
			.write(page_id!(69, 420), &[1; PAGE_BODY_SIZE], wal_index!(69, 420))
			.unwrap();
	}

	#[test]
	fn read_from_storage() {
		// expect
		let mut folder = MockDatabaseFolderApi::new();
		folder
			.expect_open_segment_file()
			.once()
			.with(eq(69))
			.returning(|_| {
				let mut segment = MockSegmentFileApi::new();
				segment
					.expect_read()
					.once()
					.with(eq(non_zero!(420)), always())
					.returning(|_, buf| {
						buf[0..3].copy_from_slice(&[1, 2, 3]);
						Ok(wal_index!(69, 420))
					});
				Ok(segment)
			});

		// given
		let storage = PhysicalStorage::new(Arc::new(folder), &Default::default());

		// when
		let mut buf = [0; 3];
		let wal_index = storage.read(page_id!(69, 420), &mut buf).unwrap();

		// then
		assert_eq!(wal_index, wal_index!(69, 420));
		assert_eq!(buf[0..3], [1, 2, 3]);
	}
}
