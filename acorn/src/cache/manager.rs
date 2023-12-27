use std::{collections::VecDeque, num::NonZeroU32};

#[derive(Debug)]
pub struct CacheManager {
	slow: VecDeque<NonZeroU32>,
	fast_cap: usize,
	fast: VecDeque<NonZeroU32>,
	graveyard_cap: usize,
	graveyard: VecDeque<NonZeroU32>,
}

impl CacheManager {
	pub fn new(length: usize) -> Self {
		debug_assert!(length >= 4);
		Self {
			slow: VecDeque::new(),
			fast_cap: length / 4,
			fast: VecDeque::new(),
			graveyard_cap: length / 2,
			graveyard: VecDeque::new(),
		}
	}

	pub fn access(&mut self, item: NonZeroU32) {
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

	pub fn reclaim(&mut self) -> Option<NonZeroU32> {
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
		mgr.access(NonZeroU32::new(1).unwrap());
		mgr.access(NonZeroU32::new(2).unwrap());
		mgr.access(NonZeroU32::new(3).unwrap());
		mgr.access(NonZeroU32::new(4).unwrap());
		mgr.access(NonZeroU32::new(5).unwrap());

		// Should immediately reclaim the tail of the fast queue
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(1).unwrap()));
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(2).unwrap()));
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(3).unwrap()));
		assert_eq!(mgr.reclaim(), None);
	}

	#[test]
	fn slow_lru_and_graveyard() {
		let mut mgr = CacheManager::new(8);

		// Flood the fast queue
		mgr.access(NonZeroU32::new(1).unwrap());
		mgr.access(NonZeroU32::new(2).unwrap());
		mgr.access(NonZeroU32::new(3).unwrap());
		mgr.access(NonZeroU32::new(69).unwrap());
		mgr.access(NonZeroU32::new(420).unwrap());

		// Reclaim to make shure 1, 2, and 3 are in the graveyard
		mgr.reclaim();
		mgr.reclaim();
		mgr.reclaim();

		// Resurrect 1, 2, and 3 from the graveyard to the slow queue
		mgr.access(NonZeroU32::new(1).unwrap());
		mgr.access(NonZeroU32::new(2).unwrap());
		mgr.access(NonZeroU32::new(3).unwrap());

		// This should influence the order of reclaiming
		mgr.access(NonZeroU32::new(1).unwrap());
		mgr.access(NonZeroU32::new(3).unwrap());

		// Should reclaim from slow queue according to LRU
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(2).unwrap()));
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(1).unwrap()));
		assert_eq!(mgr.reclaim(), Some(NonZeroU32::new(3).unwrap()));
	}
}
