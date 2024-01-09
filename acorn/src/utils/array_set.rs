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

	pub fn iter(&self) -> Iter<T> {
		self.slots.iter().flatten()
	}

	pub fn iter_mut(&mut self) -> IterMut<T> {
		self.slots.iter_mut().flatten()
	}

	pub fn clear(&mut self) {
		self.slots.clear();
	}

	fn make_space_for(&mut self, key: usize) {
		if key >= self.slots.len() {
			let extend_by = self.slots.len() - key + 1;
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

pub type Iter<'a, T> = iter::Flatten<slice::Iter<'a, Option<T>>>;
pub type IterMut<'a, T> = iter::Flatten<slice::IterMut<'a, Option<T>>>;

impl<'a, T> IntoIterator for &'a ArrayMap<T> {
	type Item = &'a T;
	type IntoIter = Iter<'a, T>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl<'a, T> IntoIterator for &'a mut ArrayMap<T> {
	type Item = &'a mut T;
	type IntoIter = IterMut<'a, T>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter_mut()
	}
}
