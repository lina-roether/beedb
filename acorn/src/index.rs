use std::{
	fmt::{self},
	hash::{Hash, Hasher},
	num::NonZeroU16,
};

#[derive(Clone, Copy)]
pub union StorageIndex {
	num: u64,
	parts: StorageIndexParts,
}

impl StorageIndex {
	#[inline]
	pub unsafe fn from_num_unchecked(num: u64) -> Self {
		Self { num }
	}

	#[inline]
	pub fn from_parts(segment_num: u32, page_num: NonZeroU16, page_index: u16) -> Self {
		Self {
			parts: StorageIndexParts {
				segment_num,
				page_num,
				page_index,
			},
		}
	}

	#[inline]
	fn as_num(&self) -> u64 {
		unsafe { self.num }
	}

	#[inline]
	pub fn segment_num(&self) -> u32 {
		unsafe { self.parts.segment_num }
	}

	#[inline]
	pub fn page_num(&self) -> NonZeroU16 {
		unsafe { self.parts.page_num }
	}

	#[inline]
	pub fn page_index(&self) -> u16 {
		unsafe { self.parts.page_index }
	}
}

impl From<StorageIndex> for u64 {
	fn from(value: StorageIndex) -> Self {
		value.as_num()
	}
}

impl fmt::Debug for StorageIndex {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"{:08x}:{:04x}:{:04x}",
			self.segment_num(),
			self.page_num(),
			self.page_index()
		)
	}
}

impl PartialEq for StorageIndex {
	fn eq(&self, other: &Self) -> bool {
		self.as_num() == other.as_num()
	}
}

impl Eq for StorageIndex {}

impl Hash for StorageIndex {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_num().hash(state);
	}
}

#[derive(Debug, Clone, Copy)]
#[repr(packed)]
struct StorageIndexParts {
	segment_num: u32,
	page_num: NonZeroU16,
	page_index: u16,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_parts() {
		let idx = StorageIndex::from_parts(0x0004_2069, NonZeroU16::new(0x0420).unwrap(), 0x0069);
		assert_eq!(idx.segment_num(), 0x0004_2069);
		assert_eq!(idx.page_num(), NonZeroU16::new(0x0420).unwrap());
		assert_eq!(idx.page_index(), 0x0069);
	}

	#[test]
	fn debug_repr() {
		let idx = StorageIndex::from_parts(0x04206942, NonZeroU16::new(0x0694).unwrap(), 0x2069);

		assert_eq!(format!("{idx:?}"), String::from("04206942:0694:2069"));
	}
}
