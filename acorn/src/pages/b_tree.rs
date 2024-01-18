use byte_view::ByteView;

use crate::id::ItemId;

#[derive(Debug, ByteView)]
#[repr(C)]
pub struct BTreeNode<K: ByteView + Ord> {
	pub pointer: ItemId,
	pub key: K,
}

#[derive(Debug, ByteView)]
#[dynamically_sized]
#[repr(C)]
pub struct BTreePage<K: ByteView + Ord> {
	pub nodes: [BTreeNode<K>],
}
