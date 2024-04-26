use musli_zerocopy::ZeroCopy;

#[derive(Debug, ZeroCopy, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct PageId {
	pub segment_num: u32,
	pub page_num: u16,
}
