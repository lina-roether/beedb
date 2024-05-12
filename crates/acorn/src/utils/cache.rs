use std::{
	collections::{HashSet, VecDeque},
	hash::Hash,
};

struct ClockItem<T> {
	value: T,
	referenced: bool,
}

struct ClockList<T> {
	items: VecDeque<ClockItem<T>>,
}

impl<T: PartialEq> ClockList<T> {
	fn new() -> Self {
		Self {
			items: VecDeque::new(),
		}
	}

	#[inline]
	fn size(&self) -> usize {
		self.items.len()
	}

	#[inline]
	fn is_empty(&self) -> bool {
		self.items.is_empty()
	}

	fn move_hand(&mut self, positions: usize) {
		self.items.rotate_left(positions);
	}

	fn insert(&mut self, value: T) {
		self.items.push_back(ClockItem {
			value,
			referenced: false,
		});
	}

	fn remove(&mut self) -> Option<ClockItem<T>> {
		let item = self.items.pop_front()?;
		Some(item)
	}

	fn current(&self) -> Option<&ClockItem<T>> {
		self.items.front()
	}

	fn access(&mut self, value: &T) -> bool {
		for item in &mut self.items {
			if item.value == *value {
				item.referenced = true;
				return true;
			}
		}
		false
	}
}

struct LruList<T> {
	items: VecDeque<T>,
	items_set: HashSet<T>,
}

impl<T: Clone + Hash + Eq> LruList<T> {
	fn new() -> Self {
		Self {
			items: VecDeque::new(),
			items_set: HashSet::new(),
		}
	}

	#[inline]
	fn len(&self) -> usize {
		self.items.len()
	}

	fn enqueue(&mut self, value: T) {
		self.items.push_back(value.clone());
		self.items_set.insert(value);
	}

	fn dequeue(&mut self) -> Option<T> {
		let value = self.items.pop_front()?;
		self.items_set.remove(&value);
		Some(value)
	}

	fn contains(&self, value: &T) -> bool {
		self.items_set.contains(value)
	}
}

/// This is an impelementation of the CAR algorithm.
/// See [Bansal et. al. 2012](https://theory.stanford.edu/~sbansal/pubs/fast04.pdf).
pub(crate) struct CacheReplacer<T> {
	recent: ClockList<T>,
	recent_history: LruList<T>,
	frequent: ClockList<T>,
	frequent_history: LruList<T>,
	recent_target_size: usize,
	size: usize,
}

impl<T: Clone + Hash + Eq> CacheReplacer<T> {
	pub fn new(size: usize) -> Self {
		Self {
			recent: ClockList::new(),
			recent_history: LruList::new(),
			frequent: ClockList::new(),
			frequent_history: LruList::new(),
			recent_target_size: 0,
			size,
		}
	}

	pub fn access(&mut self, value: &T) -> bool {
		self.recent.access(value) || self.frequent.access(value)
	}

	pub fn evict_replace(&mut self, value: T) -> Option<T> {
		let mut evicted: Option<T> = None;
		if self.cache_is_full() {
			evicted = self.evict();

			if !self.item_in_history(&value) {
				self.evict_history();
			}
		}

		self.insert(value);

		evicted
	}

	fn item_in_history(&self, value: &T) -> bool {
		self.recent_history.contains(value) || self.frequent_history.contains(value)
	}

	fn cache_is_full(&self) -> bool {
		self.recent.size() + self.frequent.size() >= self.size
	}

	fn recent_cache_is_full(&self) -> bool {
		!self.recent.is_empty() && self.recent.size() >= self.recent_target_size
	}

	fn recent_history_is_full(&self) -> bool {
		self.recent.size() + self.recent_history.len() >= self.size
	}

	fn frequent_history_is_full(&self) -> bool {
		self.recent.size()
			+ self.frequent.size()
			+ self.recent_history.len()
			+ self.frequent_history.len()
			>= self.size * 2
	}

	fn evict(&mut self) -> Option<T> {
		loop {
			if self.recent_cache_is_full() {
				let recent_head = self.recent.remove().unwrap();
				if !recent_head.referenced {
					self.frequent_history.enqueue(recent_head.value.clone());
					return Some(recent_head.value);
				} else {
					self.frequent.insert(recent_head.value);
				}
			} else {
				let frequent_head = self.frequent.remove()?;
				if !frequent_head.referenced {
					self.frequent_history.enqueue(frequent_head.value.clone());
					return Some(frequent_head.value);
				} else {
					self.frequent.insert(frequent_head.value);
				}
			}
		}
	}

	fn evict_history(&mut self) {
		if self.recent_history_is_full() {
			self.recent_history.dequeue();
		} else if self.frequent_history_is_full() {
			self.frequent_history.dequeue();
		}
	}

	fn recent_target_delta(&mut self) -> usize {
		usize::max(1, self.frequent_history.len() / self.recent_history.len())
	}

	fn increase_recent_target(&mut self) {
		self.recent_target_size = usize::min(
			self.recent_target_size + self.recent_target_delta(),
			self.size,
		);
	}

	fn decrease_recent_target(&mut self) {
		self.recent_target_size = self
			.recent_target_size
			.saturating_sub(self.recent_target_delta())
	}

	fn insert(&mut self, value: T) {
		if self.recent_history.contains(&value) {
			self.increase_recent_target();
			self.frequent.insert(value);
		} else if self.frequent_history.contains(&value) {
			self.decrease_recent_target();
			self.frequent.insert(value);
		} else {
			self.recent.insert(value);
		}
	}
}
