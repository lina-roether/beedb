use std::{
	collections::{HashMap, HashSet},
	num::NonZeroU64,
	sync::Arc,
};

#[cfg(test)]
use mockall::automock;

use crate::{
	cache::{PageCache, PageCacheApi},
	disk::wal::{self, Wal, WalApi},
	id::PageId,
};

use super::err::Error;

#[allow(clippy::needless_lifetimes)]
#[cfg_attr(test, automock)]
pub(super) trait RecoveryManagerApi {
	fn recover(&mut self) -> Result<(), Error>;

	fn track_write<'a>(
		&mut self,
		item_info: wal::ItemInfo,
		write_info: wal::WriteInfo<'a>,
	) -> Result<(), Error>;

	fn commit_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error>;

	fn cancel_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error>;
}

pub(super) struct RecoveryManager<PageCache = self::PageCache, Wal = self::Wal>
where
	PageCache: PageCacheApi,
	Wal: WalApi,
{
	page_cache: Arc<PageCache>,
	wal: Wal,
}

impl<PageCache, Wal> RecoveryManager<PageCache, Wal>
where
	PageCache: PageCacheApi,
	Wal: WalApi,
{
	pub fn new(page_cache: Arc<PageCache>, wal: Wal) -> Self {
		Self { page_cache, wal }
	}
}

impl<PageCache, Wal> RecoveryManagerApi for RecoveryManager<PageCache, Wal>
where
	PageCache: PageCacheApi,
	Wal: WalApi,
{
	fn recover(&mut self) -> Result<(), Error> {
		let mut open_transactions: HashMap<u64, NonZeroU64> = HashMap::new();
		self.fast_forward(&mut open_transactions)?;
		for (_, last_seq) in open_transactions {
			self.revert_from(last_seq)?;
		}
		Ok(())
	}

	fn track_write(
		&mut self,
		item_info: wal::ItemInfo,
		write_info: wal::WriteInfo,
	) -> Result<(), Error> {
		self.wal
			.push_write(item_info, write_info)
			.map_err(Error::WalWrite)
	}

	fn commit_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error> {
		self.wal.push_commit(item_info).map_err(Error::WalWrite)?;
		self.wal.flush().map_err(Error::WalWrite)?;
		Ok(())
	}

	fn cancel_transaction(&mut self, item_info: wal::ItemInfo) -> Result<(), Error> {
		let seq = item_info.seq;
		self.wal.push_cancel(item_info).map_err(Error::WalWrite)?;
		self.wal.flush().map_err(Error::WalWrite)?;
		self.revert_from(seq)?;
		Ok(())
	}
}

impl<PageCache, Wal> RecoveryManager<PageCache, Wal>
where
	PageCache: PageCacheApi,
	Wal: WalApi,
{
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
		page_cache: &PageCache,
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

#[cfg(test)]
mod tests {
	use crate::cache::{MockPageCacheApi, MockWriteGuard};

	use self::wal::MockWalApi;

	use super::*;

	use mockall::{predicate::*, Sequence};

	#[test]
	fn recover() {
		// expect
		let mut wal = MockWalApi::new();
		wal.expect_iter().returning(|| {
			Ok(vec![
				Ok(wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(1).unwrap(),
						prev_seq: None,
					},
					data: wal::ItemData::Write {
						page_id: PageId::new(69, 420),
						start: 0,
						before: vec![1; 16].into(),
						after: vec![2; 16].into(),
					},
				}),
				Ok(wal::Item {
					info: wal::ItemInfo {
						tid: 1,
						seq: NonZeroU64::new(2).unwrap(),
						prev_seq: None,
					},
					data: wal::ItemData::Write {
						page_id: PageId::new(25, 24),
						start: 0,
						before: vec![3; 16].into(),
						after: vec![4; 16].into(),
					},
				}),
				Ok(wal::Item {
					info: wal::ItemInfo {
						tid: 3,
						seq: NonZeroU64::new(3).unwrap(),
						prev_seq: None,
					},
					data: wal::ItemData::Write {
						page_id: PageId::new(1, 2),
						start: 0,
						before: vec![5; 16].into(),
						after: vec![6; 16].into(),
					},
				}),
				Ok(wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(4).unwrap(),
						prev_seq: NonZeroU64::new(3),
					},
					data: wal::ItemData::Cancel,
				}),
				Ok(wal::Item {
					info: wal::ItemInfo {
						tid: 0,
						seq: NonZeroU64::new(5).unwrap(),
						prev_seq: NonZeroU64::new(1),
					},
					data: wal::ItemData::Commit,
				}),
			]
			.into_iter())
		});

		let mut cache = MockPageCacheApi::new();
		cache.expect_page_size().returning(|| 16);

		let mut write_seq = Sequence::new();
		cache
			.expect_write_page()
			.with(eq(PageId::new(69, 420)))
			.returning(|_| Ok(MockWriteGuard::new(vec![2; 16].into())))
			.once()
			.in_sequence(&mut write_seq);
		cache
			.expect_write_page()
			.with(eq(PageId::new(25, 24)))
			.returning(|_| Ok(MockWriteGuard::new(vec![4; 16].into())))
			.once()
			.in_sequence(&mut write_seq);
		cache
			.expect_write_page()
			.with(eq(PageId::new(1, 2)))
			.returning(|_| Ok(MockWriteGuard::new(vec![6; 16].into())))
			.once()
			.in_sequence(&mut write_seq);
		cache
			.expect_write_page()
			.with(eq(PageId::new(1, 2)))
			.returning(|_| Ok(MockWriteGuard::new(vec![5; 16].into())))
			.once()
			.in_sequence(&mut write_seq);
		cache
			.expect_write_page()
			.with(eq(PageId::new(25, 24)))
			.returning(|_| Ok(MockWriteGuard::new(vec![3; 16].into())))
			.once()
			.in_sequence(&mut write_seq);

		// given
		let mut recv = RecoveryManager::new(Arc::new(cache), wal);

		// when
		recv.recover().unwrap();
	}
}
