use std::{mem, num::NonZero};

use crate::{
	database::pages::FreelistPage,
	storage::{PageId, TransactionApi},
};

use super::{pages::MetaPage, DatabaseError};

struct PageAllocator;

impl PageAllocator {
	const META_PAGE_ID: PageId = PageId::new_unwrap(0, 1);

	pub fn init(t: &mut impl TransactionApi) -> Result<(), DatabaseError> {
		let mut meta_page = MetaPage::new_unchecked(t.get_page_mut(Self::META_PAGE_ID)?);
		meta_page.init(Self::page_id_after(Self::META_PAGE_ID))?;
		Ok(())
	}

	pub fn alloc(t: &mut impl TransactionApi) -> Result<PageId, DatabaseError> {
		if let Some(free_page) = Self::next_free_page(t)? {
			return Ok(free_page);
		}
		Self::next_uninit_page(t)
	}

	pub fn free(t: &mut impl TransactionApi, page_id: PageId) -> Result<(), DatabaseError> {
		let meta_page = Self::meta_page(t)?;
		if let Some(freelist_head_id) = meta_page.get_freelist_head()? {
			mem::drop(meta_page);

			let mut freelist_head = FreelistPage::new(t.get_page_mut(freelist_head_id)?)?;
			if !freelist_head.is_full()? {
				freelist_head.push_item(page_id)?;
			} else {
				mem::drop(freelist_head);

				let mut new_freelist_head = FreelistPage::new_unchecked(t.get_page_mut(page_id)?);
				new_freelist_head.init()?;
				new_freelist_head.set_next_page_id(Some(freelist_head_id))?;
				mem::drop(new_freelist_head);

				let mut meta_page = Self::meta_page_mut(t)?;
				meta_page.set_freelist_head(Some(page_id))?;
			}
			return Ok(());
		}
		mem::drop(meta_page);

		let mut freelist_head = FreelistPage::new_unchecked(t.get_page_mut(page_id)?);
		freelist_head.init()?;
		mem::drop(freelist_head);

		let mut meta_page = Self::meta_page_mut(t)?;
		meta_page.set_freelist_head(Some(page_id))?;
		Ok(())
	}

	fn next_free_page(t: &mut impl TransactionApi) -> Result<Option<PageId>, DatabaseError> {
		let Some(freelist_head_id) = Self::meta_page(t)?.get_freelist_head()? else {
			return Ok(None);
		};
		let mut freelist_page = FreelistPage::new(t.get_page_mut(freelist_head_id)?)?;
		if let Some(id) = freelist_page.pop_item()? {
			return Ok(Some(id));
		}

		let new_head = freelist_page.get_next_page_id()?;
		mem::drop(freelist_page);

		Self::meta_page_mut(t)?.set_freelist_head(new_head)?;
		Ok(Some(freelist_head_id))
	}

	fn next_uninit_page(t: &mut impl TransactionApi) -> Result<PageId, DatabaseError> {
		let mut meta_page = Self::meta_page_mut(t)?;
		let page_id = meta_page.get_next_page_id()?;
		meta_page.set_next_page_id(Self::page_id_after(page_id))?;
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

	fn meta_page<T: TransactionApi>(t: &mut T) -> Result<MetaPage<T::Page<'_>>, DatabaseError> {
		MetaPage::new(t.get_page(Self::META_PAGE_ID)?)
	}

	fn meta_page_mut<T: TransactionApi>(
		t: &mut T,
	) -> Result<MetaPage<T::PageMut<'_>>, DatabaseError> {
		MetaPage::new(t.get_page_mut(Self::META_PAGE_ID)?)
	}
}

#[cfg(test)]
mod tests {
	use crate::{
		database::pages::PageKind,
		storage::{test_helpers::page_id, MockPage, MockPageMut, MockTransactionApi},
	};
	use mockall::{predicate::*, Sequence};

	use super::*;

	#[test]
	fn init() {
		// expect
		let mut t = MockTransactionApi::new();
		t.expect_get_page_mut()
			.once()
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				page.expect_write()
					.with(eq(0), eq([PageKind::FreelistMeta as u8]))
					.once()
					.returning(|_, _| Ok(()));
				page.expect_write()
					.with(eq(1), eq([0; 6]))
					.once()
					.returning(|_, _| Ok(()));
				page.expect_write()
					.once()
					.with(
						eq(7),
						eq([
							0_u32.to_ne_bytes().as_slice(),
							2_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		PageAllocator::init(&mut t).unwrap();
	}

	#[test]
	fn alloc() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (24:25)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x24_u32.to_ne_bytes().as_slice(),
								0x25_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				Ok(page)
			});

		// - access the freelist head page
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistBlock as u8]);
						Ok(())
					});
				// - read the length of the freelist head page (60)
				page.expect_read()
					.once()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&60_u16.to_ne_bytes());
						Ok(())
					});
				// - read the last item of the freelist head page (None)
				page.expect_read()
					.once()
					.with(eq(363), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[0; 6]);
						Ok(())
					});
				// - read the second to last item of the freelist head page (Some(69:420))
				page.expect_read()
					.once()
					.with(eq(357), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x69_u32.to_ne_bytes().as_slice(),
								0x420_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				// - reduce the length of the freelist page by two (b/c one empty entry was
				//   skipped)
				page.expect_write()
					.once()
					.with(eq(7), eq(58_u16.to_ne_bytes()))
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		let page_id = PageAllocator::alloc(&mut t).unwrap();

		// then
		assert_eq!(page_id, page_id!(0x69, 0x420));
	}

	#[test]
	fn alloc_with_empty_freelist_page() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (24:25)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x24_u32.to_ne_bytes().as_slice(),
								0x25_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				Ok(page)
			});

		// - access the freelist head page
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistBlock as u8]);
						Ok(())
					});
				// - read the length of the freelist head page (0)
				page.expect_read()
					.once()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&0_u16.to_ne_bytes());
						Ok(())
					});
				// - read the next page ID in the freelist (60:70)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x60_u32.to_ne_bytes().as_slice(),
								0x70_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				Ok(page)
			});

		// - access the alloc meta page
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - make the next page the new head page
				page.expect_write()
					.once()
					.with(
						eq(1),
						eq([
							0x60_u32.to_ne_bytes().as_slice(),
							0x70_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		let page_id = PageAllocator::alloc(&mut t).unwrap();

		// then
		assert_eq!(page_id, page_id!(0x24, 0x25));
	}

	#[test]
	fn alloc_with_empty_freelist() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (None)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.fill(0);
						Ok(())
					});
				Ok(page)
			});

		// - access the alloc meta page mutably
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the next uninitialized page ID (2000:3)
				page.expect_read()
					.once()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x2000_u32.to_ne_bytes().as_slice(),
								0x3_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				// - increment the next unititialized page ID to 2000:4
				page.expect_write()
					.once()
					.with(
						eq(7),
						eq([
							0x2000_u32.to_ne_bytes().as_slice(),
							0x4_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		let page_id = PageAllocator::alloc(&mut t).unwrap();

		// then
		assert_eq!(page_id, page_id!(0x2000, 0x3));
	}

	#[test]
	fn alloc_with_empty_freelist_at_segment_boundary() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (None)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.fill(0);
						Ok(())
					});
				Ok(page)
			});

		// - access the alloc meta page mutably
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the next uninitialized page ID (2000:ffff)
				page.expect_read()
					.once()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x2000_u32.to_ne_bytes().as_slice(),
								0xffff_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				// - increment the next unititialized page ID to 2001:1
				page.expect_write()
					.once()
					.with(
						eq(7),
						eq([
							0x2001_u32.to_ne_bytes().as_slice(),
							0x1_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		let page_id = PageAllocator::alloc(&mut t).unwrap();

		// then
		assert_eq!(page_id, page_id!(0x2000, 0xffff));
	}

	#[test]
	fn free() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (2000:1)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x2000_u32.to_ne_bytes().as_slice(),
								0x1_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				Ok(page)
			});

		// - access the freelist head page (2000:1)
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x2000, 0x1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistBlock as u8]);
						Ok(())
					});
				// - read the page length (2)
				page.expect_read()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&2_u16.to_ne_bytes());
						Ok(())
					});

				// - write the new free page id
				page.expect_write()
					.once()
					.with(
						eq(21),
						eq([
							0x69_u32.to_ne_bytes().as_slice(),
							0x420_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));

				// - increment the page length
				page.expect_write()
					.once()
					.with(eq(7), eq(3_u16.to_ne_bytes()))
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		PageAllocator::free(&mut t, page_id!(0x69, 0x420)).unwrap();
	}

	#[test]
	fn free_with_no_head() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (None)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.fill(0);
						Ok(())
					});
				Ok(page)
			});

		// - access the new freelist head page (69:420)
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x69, 0x420)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - set the page type
				page.expect_write()
					.once()
					.with(eq(0), eq([PageKind::FreelistBlock as u8]))
					.returning(|_, _| Ok(()));
				// - set the next freelist page id to None
				page.expect_write()
					.once()
					.with(eq(1), eq([0; 6]))
					.returning(|_, _| Ok(()));

				// - set the page length to 0
				page.expect_write()
					.once()
					.with(eq(7), eq([0; 2]))
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// - access the alloc meta page mutably
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - set the freelist head page ID to 69:420
				page.expect_write()
					.once()
					.with(
						eq(1),
						eq([
							0x69_u32.to_ne_bytes().as_slice(),
							0x420_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		PageAllocator::free(&mut t, page_id!(0x69, 0x420)).unwrap();
	}

	#[test]
	fn free_with_full_head() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - access the alloc meta page
		t.expect_get_page()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPage::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - read the freelist head page ID (2000:1)
				page.expect_read()
					.once()
					.with(eq(1), always())
					.returning(|_, buf| {
						buf.copy_from_slice(
							&[
								0x2000_u32.to_ne_bytes().as_slice(),
								0x1_u16.to_ne_bytes().as_slice(),
							]
							.concat(),
						);
						Ok(())
					});
				Ok(page)
			});

		// - access the freelist head page (2000:1)
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x2000, 0x1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - set the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistBlock as u8]);
						Ok(())
					});
				// - read the page length (2)
				page.expect_read()
					.once()
					.with(eq(7), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&(FreelistPage::<()>::NUM_SLOTS as u16).to_ne_bytes());
						Ok(())
					});
				Ok(page)
			});

		// - access the new freelist head page (69:420)
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x69, 0x420)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - set the page type
				page.expect_write()
					.once()
					.with(eq(0), eq([PageKind::FreelistBlock as u8]))
					.returning(|_, _| Ok(()));
				// - reset the next freelist page id
				page.expect_write()
					.once()
					.with(eq(1), eq([0; 6]))
					.returning(|_, _| Ok(()));
				// - set the next freelist page id to 2000:1
				page.expect_write()
					.once()
					.with(
						eq(1),
						eq([
							0x2000_u32.to_ne_bytes().as_slice(),
							0x1_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));

				// - set the page length to 0
				page.expect_write()
					.once()
					.with(eq(7), eq([0; 2]))
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// - access the alloc meta page mutably
		t.expect_get_page_mut()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)))
			.returning(|_| {
				let mut page = MockPageMut::new();
				// - check the page type
				page.expect_read()
					.once()
					.with(eq(0), always())
					.returning(|_, buf| {
						buf.copy_from_slice(&[PageKind::FreelistMeta as u8]);
						Ok(())
					});
				// - set the freelist head page ID to 69:420
				page.expect_write()
					.once()
					.with(
						eq(1),
						eq([
							0x69_u32.to_ne_bytes().as_slice(),
							0x420_u16.to_ne_bytes().as_slice(),
						]
						.concat()),
					)
					.returning(|_, _| Ok(()));
				Ok(page)
			});

		// when
		PageAllocator::free(&mut t, page_id!(0x69, 0x420)).unwrap();
	}
}
