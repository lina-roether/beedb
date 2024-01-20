use byte_view::ByteView;

use crate::id::{ItemId, PageId};

#[derive(Debug, ByteView)]
#[repr(C)]
pub struct BTreeSection<K: ByteView + Ord> {
	pub pointer: ItemId,
	pub key: K,
}

#[derive(Debug, ByteView)]
#[dynamically_sized]
#[repr(C)]
pub struct BTreePage<K: ByteView + Ord> {
	pub next_segment_num: u32,
	pub next_page_num: u16,
	pub is_leaf: u8,
	pub sections: [BTreeSection<K>],
}

impl<K: ByteView + Ord> BTreePage<K> {
	#[inline]
	pub fn next_page(&self) -> PageId {
		PageId::new(self.next_segment_num, self.next_page_num)
	}
}
