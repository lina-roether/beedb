use std::{collections::HashMap, mem, sync::Arc};

#[cfg(test)]
use mockall::automock;

use parking_lot::RwLock;
use static_assertions::assert_impl_all;

use crate::{
	consts::DEFAULT_MAX_NUM_OPEN_SEGMENTS,
	files::{
		segment::{SegmentFileApi, SegmentOp, SegmentReadOp, SegmentWriteOp},
		DatabaseFolder, DatabaseFolderApi,
	},
	utils::cache::CacheReplacer,
};

use super::{PageAddress, StorageError, WalIndex};

pub(crate) struct PhysicalStorage<DF = DatabaseFolder>
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
	pub fn new(folder: Arc<DF>, config: &PhysicalStorageConfig) -> Self {
		let descriptor_cache = RwLock::new(DescriptorCache::new(config));
		Self {
			folder,
			descriptor_cache,
		}
	}

	fn use_segment(
		&self,
		segment_num: u32,
		handler: impl FnOnce(&DF::SegmentFile) -> Result<(), StorageError>,
	) -> Result<(), StorageError> {
		let cache = self.descriptor_cache.read();
		if let Some(segment) = cache.get_descriptor(segment_num) {
			return handler(segment);
		}
		mem::drop(cache);

		let segment_file = self.folder.open_segment_file(segment_num)?;
		let mut cache_mut = self.descriptor_cache.write();
		let segment_file = cache_mut.store_descriptor(segment_num, segment_file);
		handler(segment_file)
	}
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ReadOp<'a> {
	pub page_address: PageAddress,
	pub wal_index: &'a mut Option<WalIndex>,
	pub buf: &'a mut [u8],
}

impl<'a> From<ReadOp<'a>> for SegmentReadOp<'a> {
	fn from(op: ReadOp<'a>) -> Self {
		SegmentReadOp {
			page_num: op.page_address.page_num,
			wal_index: op.wal_index,
			buf: op.buf,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteOp<'a> {
	pub wal_index: WalIndex,
	pub page_address: PageAddress,
	pub buf: &'a [u8],
}

impl<'a> From<WriteOp<'a>> for SegmentWriteOp<'a> {
	fn from(op: WriteOp<'a>) -> Self {
		SegmentWriteOp {
			page_num: op.page_address.page_num,
			wal_index: op.wal_index,
			buf: op.buf,
		}
	}
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Op<'a> {
	Read(ReadOp<'a>),
	Write(WriteOp<'a>),
}

#[cfg_attr(test, automock)]
#[allow(clippy::needless_lifetimes)]
pub(crate) trait PhysicalStorageApi {
	fn read<'a>(&self, op: ReadOp<'a>) -> Result<(), StorageError>;

	fn write<'a>(&self, op: WriteOp<'a>) -> Result<(), StorageError>;

	fn batch<'a>(&self, ops: Box<[Op<'a>]>) -> Result<(), StorageError>;
}

impl<DF: DatabaseFolderApi> PhysicalStorageApi for PhysicalStorage<DF> {
	fn read(&self, op: ReadOp) -> Result<(), StorageError> {
		self.use_segment(op.page_address.segment_num, |segment| {
			segment.read(op.into())?;
			Ok(())
		})
	}

	fn write(&self, op: WriteOp) -> Result<(), StorageError> {
		self.use_segment(op.page_address.segment_num, |segment| {
			segment.write(op.into())?;
			Ok(())
		})
	}

	fn batch(&self, ops: Box<[Op]>) -> Result<(), StorageError> {
		let mut segment_batches: HashMap<u32, Vec<SegmentOp>> = HashMap::new();
		for op in ops {
			let segment_num: u32;
			let segment_op: SegmentOp;
			match op {
				Op::Read(read_op) => {
					segment_num = read_op.page_address.segment_num;
					segment_op = SegmentOp::Read(read_op.into());
				}
				Op::Write(write_op) => {
					segment_num = write_op.page_address.segment_num;
					segment_op = SegmentOp::Write(write_op.into());
				}
			}
			segment_batches
				.entry(segment_num)
				.or_default()
				.push(segment_op);
		}

		for (segment_num, mut ops) in segment_batches.into_iter() {
			self.use_segment(segment_num, |segment| {
				segment.batch(&mut ops)?;
				Ok(())
			})?
		}

		Ok(())
	}
}

struct DescriptorCache<DF: DatabaseFolderApi> {
	descriptors: HashMap<u32, DF::SegmentFile>,
	replacer: CacheReplacer<u32>,
	max_num_open_segments: usize,
}

impl<DF: DatabaseFolderApi> DescriptorCache<DF> {
	fn new(config: &PhysicalStorageConfig) -> Self {
		let descriptors = HashMap::with_capacity(config.max_num_open_segments);
		let replacer = CacheReplacer::new(config.max_num_open_segments);
		Self {
			descriptors,
			replacer,
			max_num_open_segments: config.max_num_open_segments,
		}
	}

	pub fn get_descriptor(&self, segment_num: u32) -> Option<&DF::SegmentFile> {
		let descriptor = self.descriptors.get(&segment_num)?;
		let access_successful = self.replacer.access(&segment_num);
		debug_assert!(access_successful);

		Some(descriptor)
	}

	pub fn has_descriptor(&self, segment_num: u32) -> bool {
		self.descriptors.contains_key(&segment_num)
	}

	pub fn store_descriptor(
		&mut self,
		segment_num: u32,
		segment_file: DF::SegmentFile,
	) -> &DF::SegmentFile {
		debug_assert!(!self.descriptors.contains_key(&segment_num));

		if let Some(evicted) = self.replacer.evict_replace(segment_num) {
			self.descriptors.remove(&evicted);
		}

		self.descriptors.insert(segment_num, segment_file);
		self.descriptors.get(&segment_num).unwrap()
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		files::{
			segment::{MockSegmentFileApi, PAGE_BODY_SIZE},
			test_helpers::{page_address, wal_index},
			MockDatabaseFolderApi,
		},
		utils::test_helpers::non_zero,
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
					.withf(|op| {
						*op == SegmentWriteOp {
							page_num: non_zero!(420),
							wal_index: wal_index!(69, 420),
							buf: &[1; PAGE_BODY_SIZE],
						}
					})
					.returning(|_| Ok(()));
				Ok(segment)
			});

		// given
		let storage = PhysicalStorage::new(Arc::new(folder), &Default::default());

		// when
		storage
			.write(WriteOp {
				page_address: page_address!(69, 420),
				buf: &[1; PAGE_BODY_SIZE],
				wal_index: wal_index!(69, 420),
			})
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
					.withf(|op| op.page_num == non_zero!(420))
					.returning(|op| {
						op.buf[0..3].copy_from_slice(&[1, 2, 3]);
						*op.wal_index = Some(wal_index!(69, 420));
						Ok(())
					});
				Ok(segment)
			});

		// given
		let storage = PhysicalStorage::new(Arc::new(folder), &Default::default());

		// when
		let mut buf = [0; 3];
		let mut wal_index = None;
		storage
			.read(ReadOp {
				page_address: page_address!(69, 420),
				wal_index: &mut wal_index,
				buf: &mut buf,
			})
			.unwrap();

		// then
		assert_eq!(wal_index, Some(wal_index!(69, 420)));
		assert_eq!(buf[0..3], [1, 2, 3]);
	}
}
