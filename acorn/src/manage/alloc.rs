use parking_lot::RwLock;

use super::segment_alloc::SegmentAllocManager;

pub struct AllocManager {
	segments: RwLock<Vec<SegmentAllocManager>>,
}
