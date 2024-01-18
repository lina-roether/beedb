use byte_view::ByteView;

use crate::index::StorageIndex;

#[derive(Debug, ByteView)]
#[repr(C)]
pub struct BTreeNode<K: ByteView + Ord> {
	pub pointer: StorageIndex,
	pub key: K,
}

#[derive(Debug, ByteView)]
#[dynamically_sized]
#[repr(C)]
pub struct BTreePage<K: ByteView + Ord> {
	pub nodes: [BTreeNode<K>],
}
