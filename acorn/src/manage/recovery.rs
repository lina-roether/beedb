use std::{collections::HashMap, fs::File};

use crate::{cache::PageCache, index::PageId, wal::Wal};

use super::err::Error;

pub fn recover_from_wal(cache: &PageCache, wal: &mut Wal<File>) -> Result<(), Error> {
	let mut seq_map: HashMap<PageId, u64> = HashMap::new();

	for result in wal.iter().map_err(Error::WalRead)? {
		let (header, buf) = result.map_err(Error::WalRead)?;
		if let Some(last_seq) = seq_map.get(&header.page_id) {
			if header.seq < *last_seq {
				continue;
			}
		}
		seq_map.insert(header.page_id, header.seq);
		let mut page = cache.write_page::<[u8]>(header.page_id)?;
		page.copy_from_slice(&buf);
	}

	wal.clear().unwrap();

	Ok(())
}

#[cfg(test)]
mod tests {
	use tempfile::tempdir;

	use crate::{
		consts::DEFAULT_PAGE_SIZE,
		disk::{self, DiskStorage},
		wal,
	};

	use super::*;

	#[test]
	fn simple_recover() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let mut wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = PageCache::new(storage, 100);

		wal.log_write(0, 0, PageId::new(0, 1), &[25; DEFAULT_PAGE_SIZE as usize]);
		wal.log_write(0, 1, PageId::new(0, 2), &[69; DEFAULT_PAGE_SIZE as usize]);
		wal.log_write(0, 2, PageId::new(0, 3), &[42; DEFAULT_PAGE_SIZE as usize]);
		wal.commit(0).unwrap();

		recover_from_wal(&cache, &mut wal).unwrap();

		let page_1 = cache.read_page::<[u8]>(PageId::new(0, 1)).unwrap();
		let page_2 = cache.read_page::<[u8]>(PageId::new(0, 2)).unwrap();
		let page_3 = cache.read_page::<[u8]>(PageId::new(0, 3)).unwrap();

		assert!(page_1.iter().all(|b| *b == 25));
		assert!(page_2.iter().all(|b| *b == 69));
		assert!(page_3.iter().all(|b| *b == 42));

		assert_eq!(wal.iter().unwrap().count(), 0);
	}

	#[test]
	fn recover_with_inconsistent_sequencing() {
		let dir = tempdir().unwrap();
		DiskStorage::init(dir.path(), disk::InitParams::default()).unwrap();
		Wal::init_file(dir.path().join("writes.acnl"), wal::InitParams::default()).unwrap();

		let storage = DiskStorage::load(dir.path().into()).unwrap();
		let mut wal =
			Wal::load_file(dir.path().join("writes.acnl"), wal::LoadParams::default()).unwrap();
		let cache = PageCache::new(storage, 100);

		wal.log_write(0, 0, PageId::new(0, 1), &[25; DEFAULT_PAGE_SIZE as usize]);
		wal.log_write(0, 2, PageId::new(0, 2), &[69; DEFAULT_PAGE_SIZE as usize]);

		wal.log_write(1, 1, PageId::new(0, 2), &[10; DEFAULT_PAGE_SIZE as usize]);
		wal.commit(1).unwrap();

		wal.log_write(0, 3, PageId::new(0, 3), &[42; DEFAULT_PAGE_SIZE as usize]);
		wal.commit(0).unwrap();

		recover_from_wal(&cache, &mut wal).unwrap();

		let page_1 = cache.read_page::<[u8]>(PageId::new(0, 1)).unwrap();
		let page_2 = cache.read_page::<[u8]>(PageId::new(0, 2)).unwrap();
		let page_3 = cache.read_page::<[u8]>(PageId::new(0, 3)).unwrap();

		assert!(page_1.iter().all(|b| *b == 25));
		assert!(page_2.iter().all(|b| *b == 69));
		assert!(page_3.iter().all(|b| *b == 42));

		assert_eq!(wal.iter().unwrap().count(), 0);
	}
}
