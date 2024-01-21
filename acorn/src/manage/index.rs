use byte_view::ByteView;

use crate::{
	id::{ItemId, PageId},
	pages::BTreePage,
};

use super::err::Error;

pub mod b_tree {
	use std::mem;

	use byte_view::ViewBuf;

	use crate::manage::transaction::TransactionManager;

	use super::*;

	pub fn search<K: ByteView + Ord>(
		tm: &TransactionManager,
		root: PageId,
		key: K,
	) -> Result<Option<ItemId>, Error> {
		let mut page: ViewBuf<BTreePage<K>> = ViewBuf::new();
		tm.read(root, page.as_bytes_mut())?;

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

		search(tm, next_root, key)
	}
}
