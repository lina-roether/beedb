use std::{
	collections::{HashSet, VecDeque},
	hash::Hash,
	sync::atomic::{AtomicBool, Ordering},
};

struct ClockItem<T> {
	value: T,
	referenced: AtomicBool,
}

impl<T> ClockItem<T> {
	fn was_referenced(&self) -> bool {
		self.referenced.load(Ordering::Relaxed)
	}
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
			referenced: AtomicBool::new(false),
		});
	}

	fn remove(&mut self) -> Option<ClockItem<T>> {
		let item = self.items.pop_front()?;
		Some(item)
	}

	fn current(&self) -> Option<&ClockItem<T>> {
		self.items.front()
	}

	fn access(&self, value: &T) -> bool {
		for item in &self.items {
			if item.value == *value {
				item.referenced.store(true, Ordering::Relaxed);
				return true;
			}
		}
		false
	}

	fn contains(&self, value: &T) -> bool {
		for item in &self.items {
			if item.value == *value {
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
	/// A clock containing recently added values
	recent: ClockList<T>,

	/// A LRU list containing values recently dropped from `recent`
	recent_history: LruList<T>,

	/// A clock containing values that are considered frequently used
	frequent: ClockList<T>,

	/// A LRU list containing values recently dropped from `frequent`
	frequent_history: LruList<T>,

	/// The target size for `recent`.
	recent_target_size: usize,

	/// The total cache size
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

	/// Track an access to the given value
	pub fn access(&self, value: &T) -> bool {
		// Mark the corresponding page as referenced.
		self.recent.access(value) || self.frequent.access(value)
	}

	/// Insert a value into the cache, potentially evicting a value to make
	/// space.
	pub fn evict_replace(&mut self, value: T) -> Option<T> {
		debug_assert!(!self.recent.contains(&value) && !self.frequent.contains(&value));

		let mut evicted: Option<T> = None;

		if self.cache_is_full() {
			// If the cache is full, we have to evict a value.
			evicted = self.evict();

			if !self.value_in_history(&value) {
				// If the value doesn't appear in the history lists, it will have to be added,
				// meaning that we may have to make space for it.
				self.maybe_evict_history();
			}
		}

		self.insert(value);

		evicted
	}

	/// Checks wether one of the history lists contains the value.
	fn value_in_history(&self, value: &T) -> bool {
		self.recent_history.contains(value) || self.frequent_history.contains(value)
	}

	/// Checks whether the active cache clocks can be extended without violating
	/// their size requirement: `|recent| + |frequent| <= size`
	fn cache_is_full(&self) -> bool {
		self.recent.size() + self.frequent.size() >= self.size
	}

	/// Checks whether the recent cache clock can be extended without exceeding
	/// `recent_target_size`.
	fn recent_cache_is_full(&self) -> bool {
		!self.recent.is_empty() && self.recent.size() >= self.recent_target_size
	}

	/// Checks whether the recent history LRU can be extended without violating
	/// its size requirement: `|recent| + |recent_history| <= size`
	fn recent_history_is_full(&self) -> bool {
		self.recent.size() + self.recent_history.len() >= self.size
	}

	/// Checks whether the frequent history LRU can be extended without
	/// violating its size requirement: `|recent| + |frequent| +
	/// |recent_history| + |frequent_history| <= 2 * size`
	fn frequent_history_is_full(&self) -> bool {
		self.recent.size()
			+ self.frequent.size()
			+ self.recent_history.len()
			+ self.frequent_history.len()
			>= self.size * 2
	}

	/// Evicts a value from the cache, unless the cache is empty
	fn evict(&mut self) -> Option<T> {
		// We loop until we find a suitable item.
		loop {
			if self.recent_cache_is_full() {
				// If the recent clock is full, we want to look at its head.

				let recent_head = self.recent.remove().unwrap();
				if !recent_head.was_referenced() {
					// The recent head item was not recently referenced! We evict it, and add it to
					// the history.
					self.frequent_history.enqueue(recent_head.value.clone());
					return Some(recent_head.value);
				} else {
					// The recent head item was recently referenced. We promote it to the frequent
					// clock, where it can get a second chance.
					self.frequent.insert(recent_head.value);
				}
			} else {
				// Otherwise, we look at the frequent clock so that `recent_target_size` is
				// maintained.

				let frequent_head = self.frequent.remove()?;
				if !frequent_head.was_referenced() {
					// The frequent head item was not recently referenced! We evict it, and add it
					// to the history.
					self.frequent_history.enqueue(frequent_head.value.clone());
					return Some(frequent_head.value);
				} else {
					// The frequent head item was recently referenced. We re-add it to the frequent
					// clock, giving it a second chance.
					self.frequent.insert(frequent_head.value);
				}
			}
		}
	}

	/// Evicts an item from one of the history LRUs if they are full
	fn maybe_evict_history(&mut self) {
		if self.recent_history_is_full() {
			self.recent_history.dequeue();
		} else if self.frequent_history_is_full() {
			self.frequent_history.dequeue();
		}
	}

	/// The amount by which `recent_target_size` canges each time it is
	/// increased/decreased.
	fn recent_target_delta(&mut self) -> usize {
		// We want to change it by at least one, but if there is a lot more traffic on
		// the frequent clock than on the recent clock, we should increase it by more.
		// The goal is to get the two roughly equal.
		usize::max(1, self.frequent_history.len() / self.recent_history.len())
	}

	/// Increases `recent_target_size`.
	fn increase_recent_target(&mut self) {
		self.recent_target_size = usize::min(
			self.recent_target_size + self.recent_target_delta(),
			self.size,
		);
	}

	/// Decreases `recent_target_size`.
	fn decrease_recent_target(&mut self) {
		self.recent_target_size = self
			.recent_target_size
			.saturating_sub(self.recent_target_delta())
	}

	/// Inserts a value into the cache.
	fn insert(&mut self, value: T) {
		if self.recent_history.contains(&value) {
			// The value was only recently evicted from `recent`, so we might want `recent`
			// to be bigger. We increase the `recent` target size, and add it to `frequent`.
			self.increase_recent_target();
			self.frequent.insert(value);
		} else if self.frequent_history.contains(&value) {
			// The value was only recently evicted from `frequent`, so we might want
			// `frequent` to be bigger. We decrease the `recent` target size, and add it to
			// `frequent`.
			self.decrease_recent_target();
			self.frequent.insert(value);
		} else {
			// The value is not known to have been recently evicted. We add it to `recent`.
			self.recent.insert(value);
		}
	}
}
