use std::{iter, slice};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayMap<T> {
	slots: Vec<Option<T>>,
}

impl<T> ArrayMap<T> {
	pub fn new() -> Self {
		Self { slots: Vec::new() }
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			slots: Vec::with_capacity(capacity),
		}
	}

	pub fn insert(&mut self, key: usize, value: T) -> Option<T> {
		self.make_space_for(key);
		self.slots[key].replace(value)
	}

	pub fn get(&self, key: usize) -> Option<&T> {
		self.slots.get(key)?.as_ref()
	}

	pub fn has(&self, key: usize) -> bool {
		self.get(key).is_some()
	}

	pub fn delete(&mut self, key: usize) -> Option<T> {
		self.slots.get_mut(key)?.take()
	}

	pub fn iter(&self) -> Iter<T> {
		Iter::new(&self.slots)
	}

	pub fn iter_mut(&mut self) -> IterMut<T> {
		IterMut::new(&mut self.slots)
	}

	pub fn clear(&mut self) {
		self.slots.clear();
	}

	fn make_space_for(&mut self, key: usize) {
		if key >= self.slots.len() {
			let extend_by = key - self.slots.len() + 1;
			self.slots
				.extend(iter::repeat_with(|| None).take(extend_by))
		}
	}
}

impl<T> Default for ArrayMap<T> {
	fn default() -> Self {
		Self::new()
	}
}

pub struct Iter<'a, T> {
	key: usize,
	slots: slice::Iter<'a, Option<T>>,
}

impl<'a, T> Iter<'a, T> {
	fn new(slots: &'a [Option<T>]) -> Self {
		Self {
			key: 0,
			slots: slots.iter(),
		}
	}
}

impl<'a, T> Iterator for Iter<'a, T> {
	type Item = (usize, &'a T);

	fn next(&mut self) -> Option<Self::Item> {
		for slot in self.slots.by_ref() {
			let key = self.key;
			self.key += 1;
			if let Some(item) = slot {
				return Some((key, item));
			}
		}
		None
	}
}

pub struct IterMut<'a, T> {
	key: usize,
	slots: slice::IterMut<'a, Option<T>>,
}

impl<'a, T> IterMut<'a, T> {
	fn new(slots: &'a mut [Option<T>]) -> Self {
		Self {
			key: 0,
			slots: slots.iter_mut(),
		}
	}
}

impl<'a, T> Iterator for IterMut<'a, T> {
	type Item = (usize, &'a mut T);

	fn next(&mut self) -> Option<Self::Item> {
		for slot in self.slots.by_ref() {
			let key = self.key;
			self.key += 1;
			if let Some(item) = slot {
				return Some((key, item));
			}
		}
		None
	}
}

impl<'a, T> IntoIterator for &'a ArrayMap<T> {
	type Item = (usize, &'a T);
	type IntoIter = Iter<'a, T>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl<'a, T> IntoIterator for &'a mut ArrayMap<T> {
	type Item = (usize, &'a mut T);
	type IntoIter = IterMut<'a, T>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_mut()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn simple_insert_get() {
		let mut map: ArrayMap<&'static str> = ArrayMap::new();
		map.insert(69, "Some value");
		map.insert(420, "Some other value");

		assert_eq!(map.get(69), Some(&"Some value"));
		assert_eq!(map.get(420), Some(&"Some other value"));
	}

	#[test]
	fn try_get_non_existent_key() {
		let map: ArrayMap<u32> = ArrayMap::new();

		assert_eq!(map.get(2), None);
		assert_eq!(map.get(0), None);
	}

	#[test]
	fn overwrite_values() {
		let mut map: ArrayMap<&'static str> = ArrayMap::new();
		map.insert(25, "AAAAAAAAAAAAA");
		map.insert(25, "BBBBBBBB");

		assert_eq!(map.get(25), Some(&"BBBBBBBB"));
	}

	#[test]
	fn delete_values() {
		let mut map: ArrayMap<&'static str> = ArrayMap::new();
		map.insert(25, "That's me :)");
		map.delete(25);

		assert_eq!(map.get(25), None);
	}

	#[test]
	fn iter_items() {
		let mut map: ArrayMap<u32> = ArrayMap::new();
		map.insert(25, 69);
		map.insert(69, 420);
		map.insert(420, 25);

		let mut iter = map.iter();
		assert_eq!(iter.next(), Some((25, &69)));
		assert_eq!(iter.next(), Some((69, &420)));
		assert_eq!(iter.next(), Some((420, &25)));
		assert_eq!(iter.next(), None);
	}

	#[test]
	fn iter_items_mut() {
		let mut map: ArrayMap<u32> = ArrayMap::new();
		map.insert(25, 69);
		map.insert(69, 420);
		map.insert(420, 25);

		let mut iter = map.iter_mut();
		assert_eq!(iter.next(), Some((25, &mut 69)));
		assert_eq!(iter.next(), Some((69, &mut 420)));
		assert_eq!(iter.next(), Some((420, &mut 25)));
		assert_eq!(iter.next(), None);
	}
}
