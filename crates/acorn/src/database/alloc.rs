use std::num::NonZero;

use crate::{
	database::pages::FreelistPage,
	storage::{PageId, ReadPage, WritePage},
};

use super::{pages::MetaPage, DatabaseError};

struct PageAllocator;

impl PageAllocator {
	const META_PAGE_ID: PageId = PageId::new(0, unsafe { NonZero::new_unchecked(1) });

	pub fn init(t: &mut impl WritePage) -> Result<(), DatabaseError> {
		MetaPage::write_freelist_head(Self::META_PAGE_ID, t, None)?;
		MetaPage::write_next_page_id(
			Self::META_PAGE_ID,
			t,
			Self::page_id_after(Self::META_PAGE_ID),
		)?;
		Ok(())
	}

	fn next_free_page(
		t: &mut (impl ReadPage + WritePage),
	) -> Result<Option<PageId>, DatabaseError> {
		let Some(mut freelist_head) = MetaPage::read_freelist_head(Self::META_PAGE_ID, &*t)? else {
			return Ok(None);
		};

		while FreelistPage::read_length(freelist_head, &*t)? == 0 {
			todo!()
		}

		todo!()
	}

	fn next_uninit_page(t: &mut (impl ReadPage + WritePage)) -> Result<PageId, DatabaseError> {
		let page_id = MetaPage::read_next_page_id(Self::META_PAGE_ID, &*t)?;
		MetaPage::write_next_page_id(Self::META_PAGE_ID, t, Self::page_id_after(page_id))?;
		Ok(page_id)
	}

	fn page_id_after(page_id: PageId) -> PageId {
		if page_id.page_num.get() == u16::MAX {
			PageId::new(
				page_id
					.segment_num
					.checked_add(1)
					.expect("You've somehow managed to exhaust the space of page IDs ¯\\_(ツ)_/¯"),
				NonZero::new(1).unwrap(),
			)
		} else {
			PageId::new(
				page_id.segment_num,
				page_id.page_num.checked_add(1).unwrap(),
			)
		}
	}
}
