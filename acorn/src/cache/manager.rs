use std::collections::VecDeque;

use crate::id::PageId;

#[derive(Debug)]
pub struct CacheManager {
	slow: VecDeque<PageId>,
	fast_cap: usize,
	fast: VecDeque<PageId>,
	graveyard_cap: usize,
	graveyard: VecDeque<PageId>,
}

impl CacheManager {
	pub fn new(length: usize) -> Self {
		Self {
			slow: VecDeque::new(),
			fast_cap: length / 4,
			fast: VecDeque::new(),
			graveyard_cap: length / 2,
			graveyard: VecDeque::new(),
		}
	}

	pub fn access(&mut self, item: PageId) {
		if self.fast.contains(&item) {
			return;
		}
		if let Some(index) = self.slow.iter().position(|v| *v == item) {
			self.slow.remove(index);
			self.slow.push_front(item);
			return;
		}
		if self.graveyard.contains(&item) {
			self.slow.push_front(item);
			return;
		}
		self.fast.push_front(item);
	}

	pub fn reclaim(&mut self) -> Option<PageId> {
		if self.fast.len() > self.fast_cap {
			let reclaimed = self.fast.pop_back().unwrap();
			self.graveyard.push_front(reclaimed);
			if self.graveyard.len() > self.graveyard_cap {
				self.graveyard.pop_back();
			}
			return Some(reclaimed);
		}

		self.slow.pop_back()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn fast_fifo() {
		let mut mgr = CacheManager::new(8);

		// Flood the fast queue
		mgr.access(PageId::new(0, 1));
		mgr.access(PageId::new(0, 2));
		mgr.access(PageId::new(0, 3));
		mgr.access(PageId::new(0, 4));
		mgr.access(PageId::new(0, 5));

		// Should immediately reclaim the tail of the fast queue
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 1)));
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 2)));
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 3)));
		assert_eq!(mgr.reclaim(), None);
	}

	#[test]
	fn slow_lru_and_graveyard() {
		let mut mgr = CacheManager::new(8);

		// Flood the fast queue
		mgr.access(PageId::new(0, 1));
		mgr.access(PageId::new(0, 2));
		mgr.access(PageId::new(0, 3));
		mgr.access(PageId::new(0, 69));
		mgr.access(PageId::new(0, 420));

		// Reclaim to make shure 1, 2, and 3 are in the graveyard
		mgr.reclaim();
		mgr.reclaim();
		mgr.reclaim();

		// Resurrect 1, 2, and 3 from the graveyard to the slow queue
		mgr.access(PageId::new(0, 1));
		mgr.access(PageId::new(0, 2));
		mgr.access(PageId::new(0, 3));

		// This should influence the order of reclaiming
		mgr.access(PageId::new(0, 1));
		mgr.access(PageId::new(0, 3));

		// Should reclaim from slow queue according to LRU
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 2)));
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 1)));
		assert_eq!(mgr.reclaim(), Some(PageId::new(0, 3)));
	}
}
