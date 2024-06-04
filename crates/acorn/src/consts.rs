use std::time::Duration;

use crate::utils::units::{GIB, KIB};

pub(crate) const PAGE_SIZE: usize = 32 * KIB;
pub(crate) const DEFAULT_MAX_NUM_OPEN_SEGMENTS: usize = 512;
pub(crate) const DEFAULT_MAX_WAL_GENERATION_SIZE: usize = 4 * GIB;
pub(crate) const DEFAULT_PAGE_CACHE_SIZE: usize = 2 * GIB;
pub(crate) const DEFAULT_MAX_DIRTY_PAGES: f32 = 0.2;
pub(crate) const DEFAULT_NUM_WORKERS: usize = 2;
pub(crate) const DEFAULT_CHECKPOINT_PERIOD: Duration = Duration::from_mins(1);
pub(crate) const DEFAULT_FLUSH_PERIOD: Duration = Duration::from_mins(3);
pub(crate) const SMALL_STRING_SIZE: usize = 16;
