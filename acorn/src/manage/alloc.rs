use std::sync::Arc;

use parking_lot::RwLock;

use crate::{index::PageId, utils::array_map::ArrayMap};

use super::{rw::PageRwManager, segment_alloc::SegmentAllocManager};

pub struct AllocManager {
	segments: RwLock<ArrayMap<SegmentAllocManager>>,
	free_stack: RwLock<Vec<u32>>,
	rw_mgr: Arc<PageRwManager>,
}

impl AllocManager {
	pub fn new(rw_mgr: Arc<PageRwManager>) -> Self {
		Self {
			segments: RwLock::new(ArrayMap::new()),
			free_stack: RwLock::new(Vec::new()),
			rw_mgr,
		}
	}

	pub fn alloc_page(&self) -> PageId {
		todo!()
	}

	fn peek_free_stack(&self) -> Option<u32> {
		let free_stack = self.free_stack.read();
		free_stack.last().copied()
	}
}
