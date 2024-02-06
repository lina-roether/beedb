use std::{
	collections::{HashMap, HashSet},
	fs::File,
	num::NonZeroU64,
	sync::Arc,
};

use crate::{
	cache::PageCache,
	disk::{
		storage::StorageApi,
		wal::{self, Wal},
	},
	id::PageId,
};

use super::err::Error;

pub(super) struct RecoveryManager<Storage>
where
	Storage: StorageApi,
{
	page_cache: Arc<PageCache<Storage>>,
	wal: Wal<File>,
}

impl<Storage> RecoveryManager<Storage>
where
	Storage: StorageApi,
{
	pub fn new(page_cache: Arc<PageCache<Storage>>, wal: Wal<File>) -> Self {
		Self { page_cache, wal }
	}

	pub fn recover(&mut self) -> Result<(), Error> {
		let mut open_transactions: HashMap<u64, NonZeroU64> = HashMap::new();
		self.fast_forward(&mut open_transactions)?;
		for (_, last_seq) in open_transactions {
			self.revert_from(last_seq)?;
		}
		Ok(())
	}

	pub fn track_write(
		&mut self,
		item_info: wal::ItemInfo,
		write_info: wal::WriteInfo,
	) -> Result<(), Error> {
		self.wal
			.push_write(item_info, write_info)
			.map_err(Error::WalWrite)
	}

	pub fn commit_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error> {
		self.wal.push_commit(item_info).map_err(Error::WalWrite)?;
		self.wal.flush().map_err(Error::WalWrite)?;
		Ok(())
	}

	pub fn cancel_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error> {
		let seq = item_info.seq;
		self.wal.push_cancel(item_info).map_err(Error::WalWrite)?;
		self.wal.flush().map_err(Error::WalWrite)?;
		self.revert_from(seq)?;
		Ok(())
	}

	fn fast_forward(
		&mut self,
		open_transactions: &mut HashMap<u64, NonZeroU64>,
	) -> Result<(), Error> {
		let mut revert: HashSet<NonZeroU64> = HashSet::new();
		for item_result in self.wal.iter()? {
			let item = item_result?;
			open_transactions.insert(item.info.tid, item.info.seq);
			match item.data {
				wal::ItemData::Write {
					page_id,
					start,
					after,
					..
				} => {
					Self::apply_write(&self.page_cache, page_id, start, &after)?;
				}
				wal::ItemData::Commit => {
					open_transactions.remove(&item.info.tid);
				}
				wal::ItemData::Cancel => {
					if let Some(last_seq) = open_transactions.remove(&item.info.tid) {
						revert.insert(last_seq);
					}
				}
			}
		}

		for seq in revert {
			self.revert_from(seq)?;
		}

		Ok(())
	}

	fn revert_from(&mut self, seq: NonZeroU64) -> Result<(), Error> {
		for item_result in self.wal.retrace_transaction(seq)? {
			let item = item_result?;
			let wal::ItemData::Write {
				page_id,
				start,
				before,
				..
			} = item.data
			else {
				continue;
			};
			Self::apply_write(&self.page_cache, page_id, start, &before)?;
		}
		Ok(())
	}

	fn apply_write(
		page_cache: &PageCache<Storage>,
		page_id: PageId,
		start: u16,
		data: &[u8],
	) -> Result<(), Error> {
		let mut page = page_cache.write_page(page_id)?;
		let range = (start as usize)..(start as usize + data.len());
		page[range].copy_from_slice(data);
		Ok(())
	}
}
