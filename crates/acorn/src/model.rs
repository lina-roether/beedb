use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct PageId {
	pub segment_num: u32,
	pub page_num: u16,
}
