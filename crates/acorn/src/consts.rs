use crate::utils::units::{GIB, KIB};

pub(crate) const PAGE_SIZE: usize = 16 * KIB;
pub(crate) const DEFAULT_MAX_NUM_OPEN_SEGMENTS: usize = 512;
pub(crate) const DEFAULT_MAX_WAL_GENERATION_SIZE: usize = 4 * GIB;
pub(crate) const DEFAULT_PAGE_CACHE_SIZE: usize = 2 * GIB;
pub(crate) const DEFAULT_NUM_WORKERS: usize = 2;
