use num_enum::{IntoPrimitive, TryFromPrimitive};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub(crate) struct ItemHeader {
	item_type: u8,
	flags: u8,
	data_length: u16,
	crc32: u32,
	transaction_id: u64,
	sequence_num: u64,
}

#[derive(Debug, AsBytes, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub(crate) enum ItemType {
	Write = 0,
	Commit = 1,
	Undo = 2,
}

#[derive(Debug)]
pub(crate) struct ItemMeta {
	item_type: ItemType,
	data_length: usize,
	transaction_id: u32,
	sequence_num: u64,
}
