use parking_lot::RwLock;

use crate::utils::array_map::ArrayMap;

use super::segment_alloc::SegmentAllocManager;

pub struct AllocManager {
	state: RwLock<State>,
}

impl AllocManager {
	pub fn new() -> Self {
		Self {
			state: RwLock::new(State {
				segments: ArrayMap::new(),
				free_stack: Vec::new(),
			}),
		}
	}
}

struct State {
	segments: ArrayMap<SegmentAllocManager>,
	free_stack: Vec<u32>,
}
