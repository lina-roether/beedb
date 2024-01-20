use byte_view::ByteView;

use crate::{
	id::{ItemId, PageId},
	pages::BTreePage,
};

use super::{err::Error, rw::PageRwManager};

pub mod b_tree {
	use std::mem;

	use super::*;

	pub fn search<K: ByteView + Ord>(
		rw_mgr: &PageRwManager,
		root: PageId,
		key: K,
	) -> Result<Option<ItemId>, Error> {
		let page = rw_mgr.read_page::<BTreePage<K>>(root)?;

		let mut pointer = None;
		for section in &page.sections {
			if key < section.key {
				pointer = Some(section.pointer)
			}
		}

		if page.is_leaf != 0 {
			return Ok(pointer);
		}

		let next_root = pointer
			.map(|p| p.page_id())
			.unwrap_or_else(|| page.next_page());

		mem::drop(page);

		search(rw_mgr, next_root, key)
	}
}
