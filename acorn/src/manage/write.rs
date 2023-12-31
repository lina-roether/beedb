use std::sync::Arc;

use static_assertions::assert_impl_all;

use crate::cache::PageCache;

pub struct WriteManager {
	cache: Arc<PageCache>,
}

assert_impl_all!(WriteManager: Sync);
