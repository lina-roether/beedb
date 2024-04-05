use std::{
	borrow::Cow,
	collections::HashMap,
	io,
	num::{NonZeroU32, NonZeroU64},
	sync::atomic::AtomicBool,
	vec,
};

use mockall::automock;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};

use crate::{
	disk::wal::{self, CursorApi, WalApi},
	id::PageId,
	pages::WriteOp,
};

pub(super) struct WriteEntry<'a> {
	pub start: usize,
	pub before: Cow<'a, [u8]>,
	pub after: Cow<'a, [u8]>,
}

#[automock(
    type Rewind = vec::IntoIter<Result<(PageId, WriteOp<'static>), wal::ReadError>>;
    type Apply = vec::IntoIter<Result<(PageId, WriteOp<'static>), wal::ReadError>>;
    type Recover = vec::IntoIter<Result<(PageId, WriteOp<'static>), wal::ReadError>>;
)]
pub(super) trait WriteQueueApi {
	type Rewind<'a>: Iterator<Item = Result<(PageId, WriteOp<'static>), wal::ReadError>> + 'a;
	type Apply<'a>: Iterator<Item = Result<(PageId, WriteOp<'static>), wal::ReadError>> + 'a;
	type Recover<'a>: Iterator<Item = Result<(PageId, WriteOp<'static>), wal::ReadError>> + 'a;

	fn append_write<'a>(tid: u64, write: WriteEntry<'a>) -> Result<(), io::Error>;

	fn apply<'a>(tid: u64) -> Result<Self::Apply<'a>, wal::ReadError>;

	fn rewind<'a>(tid: u64) -> Result<Self::Rewind<'a>, wal::ReadError>;

	fn recover<'a>() -> Result<Self::Recover<'a>, wal::ReadError>;
}

pub(super) struct WriteQueue<Wal>
where
	Wal: WalApi,
{
	is_flushing: AtomicBool,
	front_wal: Mutex<Wal>,
	back_wal: Mutex<Wal>,
	transaction_start: RwLock<HashMap<u64, u32>>,
}

pub(super) struct Apply<'a, Wal>
where
	Wal: WalApi,
{
	_wal_guard: MutexGuard<'a, Wal>,
	cursor: Wal::Cursor<'a>,
}

impl<'a, Wal> Iterator for Apply<'a, Wal>
where
	Wal: WalApi,
{
	type Item = Result<(PageId, WriteOp<'static>), wal::ReadError>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = match self.cursor.next().transpose()? {
				Ok(item) => item,
				Err(err) => return Some(Err(err)),
			};
			match item.data {
				wal::ItemData::Write {
					page_id,
					start,
					after,
					..
				} => {
					return Some(Ok((
						page_id,
						WriteOp::new(start.into(), Cow::Owned(after.into())),
					)))
				}
				_ => continue,
			}
		}
	}
}

pub(super) struct Rewind<'a, Wal>
where
	Wal: WalApi,
{
	_wal_guard: MutexGuard<'a, Wal>,
	tid: u64,
	start: NonZeroU64,
	cursor: Wal::Cursor<'a>,
}

impl<'a, Wal> Iterator for Rewind<'a, Wal>
where
	Wal: WalApi,
{
	type Item = Result<(PageId, WriteOp<'static>), wal::ReadError>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = match self.cursor.prev().transpose()? {
				Ok(item) => item,
				Err(err) => return Some(Err(err)),
			};

			if item.info.tid != self.tid {
				continue;
			}

			if item.info.seq < self.start {
				return None;
			}

			match item.data {
				wal::ItemData::Write {
					page_id,
					start,
					before,
					..
				} => {
					return Some(Ok((
						page_id,
						WriteOp::new(start.into(), Cow::Owned(before.into())),
					)))
				}
				_ => continue,
			}
		}
	}
}
