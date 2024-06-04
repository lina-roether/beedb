use std::num::NonZero;

use crate::{
	database::pages::FreelistPage,
	storage::{PageId, ReadPage, WritePage},
};

use super::{pages::MetaPage, DatabaseError};

struct PageAllocator;

impl PageAllocator {
	const META_PAGE: MetaPage = MetaPage::new(PageId::new(0, unsafe { NonZero::new_unchecked(1) }));

	pub fn init(mut t: impl WritePage) -> Result<(), DatabaseError> {
		Self::META_PAGE.set_freelist_head(&mut t, None)?;
		Self::META_PAGE.set_next_page_id(&mut t, Self::page_id_after(Self::META_PAGE.page_id()))?;
		Ok(())
	}

	pub fn alloc(mut t: impl ReadPage + WritePage) -> Result<PageId, DatabaseError> {
		if let Some(free_page) = Self::next_free_page(&mut t)? {
			return Ok(free_page);
		}
		Self::next_uninit_page(t)
	}

	pub fn free(mut t: impl ReadPage + WritePage, page_id: PageId) -> Result<(), DatabaseError> {
		if let Some(freelist_head_id) = Self::META_PAGE.get_freelist_head(&t)? {
			let freelist_head = FreelistPage::new(freelist_head_id);
			let length = freelist_head.get_length(&t)?;
			if length < FreelistPage::NUM_SLOTS {
				freelist_head.set_item(&mut t, length, Some(page_id))?;
			} else {
				Self::init_freelist_page(&mut t, page_id, Some(freelist_head.page_id()))?;
				Self::META_PAGE.set_freelist_head(&mut t, Some(page_id))?;
			}
			return Ok(());
		}

		Self::init_freelist_page(&mut t, page_id, None)?;
		Self::META_PAGE.set_freelist_head(&mut t, Some(page_id))?;
		Ok(())
	}

	fn init_freelist_page(
		mut t: impl ReadPage + WritePage,
		page_id: PageId,
		next_page: Option<PageId>,
	) -> Result<(), DatabaseError> {
		let freelist_page = FreelistPage::new(page_id);
		freelist_page.set_length(&mut t, 0)?;
		freelist_page.set_length(&mut t, 0)?;
		freelist_page.set_next_page_id(&mut t, next_page)?;
		Ok(())
	}

	fn next_free_page(mut t: impl ReadPage + WritePage) -> Result<Option<PageId>, DatabaseError> {
		let Some(freelist_head_id) = Self::META_PAGE.get_freelist_head(&t)? else {
			return Ok(None);
		};
		let freelist_page = FreelistPage::new(freelist_head_id);
		let mut length = freelist_page.get_length(&t)?;

		while length != 0 {
			length -= 1;
			if let Some(id) = freelist_page.get_item(&t, length)? {
				freelist_page.set_length(&mut t, length)?;
				return Ok(Some(id));
			}
		}

		let new_head = freelist_page.get_next_page_id(&t)?;
		Self::META_PAGE.set_freelist_head(&mut t, new_head)?;
		Ok(Some(freelist_page.page_id()))
	}

	fn next_uninit_page(mut t: impl ReadPage + WritePage) -> Result<PageId, DatabaseError> {
		let page_id = Self::META_PAGE.get_next_page_id(&t)?;
		Self::META_PAGE.set_next_page_id(&mut t, Self::page_id_after(page_id))?;
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

#[cfg(test)]
mod tests {
	use crate::storage::{test_helpers::page_id, MockTransactionApi};
	use mockall::{predicate::*, Sequence};

	use super::*;

	#[test]
	fn init() {
		// expect
		let mut t = MockTransactionApi::new();
		t.expect_write()
			.once()
			.with(eq(page_id!(0, 1)), eq(0), eq([0; 6]))
			.returning(|_, _, _| Ok(()));
		t.expect_write()
			.once()
			.with(
				eq(page_id!(0, 1)),
				eq(6),
				eq([
					0_u32.to_ne_bytes().as_slice(),
					2_u16.to_ne_bytes().as_slice(),
				]
				.concat()),
			)
			.returning(|_, _, _| Ok(()));

		// when
		PageAllocator::init(t).unwrap();
	}

	#[test]
	fn alloc() {
		// expect
		let mut t = MockTransactionApi::new();
		let mut seq = Sequence::new();

		// - read the freelist head page ID (24:25)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)), eq(0), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(
					&[
						0x24_u32.to_ne_bytes().as_slice(),
						0x25_u16.to_ne_bytes().as_slice(),
					]
					.concat(),
				);
				Ok(())
			});

		// - read the length of the freelist head page (60)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(6), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(&60_u16.to_ne_bytes());
				Ok(())
			});

		// - read the last item of the freelist head page (None)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(362), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(&[0; 6]);
				Ok(())
			});

		// - read the second to last item of the freelist head page (Some(69:420))
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(356), always())
			.returning(|_, _, buf| {
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
		t.expect_write()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(6), eq(58_u16.to_ne_bytes()))
			.returning(|_, _, _| Ok(()));

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

		// - read the freelist head page ID (24:25)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0, 1)), eq(0), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(
					&[
						0x24_u32.to_ne_bytes().as_slice(),
						0x25_u16.to_ne_bytes().as_slice(),
					]
					.concat(),
				);
				Ok(())
			});

		// - read the length of the freelist head page (0)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(6), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(&0_u16.to_ne_bytes());
				Ok(())
			});

		// - read the next page ID in the freelist (60:70)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x24, 0x25)), eq(0), always())
			.returning(|_, _, buf| {
				buf.copy_from_slice(
					&[
						0x60_u32.to_ne_bytes().as_slice(),
						0x70_u16.to_ne_bytes().as_slice(),
					]
					.concat(),
				);
				Ok(())
			});

		// - make the next page the new head page
		t.expect_write()
			.once()
			.in_sequence(&mut seq)
			.with(
				eq(page_id!(0x0, 0x1)),
				eq(0),
				eq([
					0x60_u32.to_ne_bytes().as_slice(),
					0x70_u16.to_ne_bytes().as_slice(),
				]
				.concat()),
			)
			.returning(|_, _, _| Ok(()));

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

		// - read the freelist head page ID (None)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x0, 0x1)), eq(0), always())
			.returning(|_, _, buf| {
				buf.fill(0);
				Ok(())
			});

		// - read the next uninitialized page ID (2000:3)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x0, 0x1)), eq(6), always())
			.returning(|_, _, buf| {
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
		t.expect_write()
			.once()
			.in_sequence(&mut seq)
			.with(
				eq(page_id!(0x0, 0x1)),
				eq(6),
				eq([
					0x2000_u32.to_ne_bytes().as_slice(),
					0x4_u16.to_ne_bytes().as_slice(),
				]
				.concat()),
			)
			.returning(|_, _, _| Ok(()));

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

		// - read the freelist head page ID (None)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x0, 0x1)), eq(0), always())
			.returning(|_, _, buf| {
				buf.fill(0);
				Ok(())
			});

		// - read the next uninitialized page ID (2000:ffff)
		t.expect_read()
			.once()
			.in_sequence(&mut seq)
			.with(eq(page_id!(0x0, 0x1)), eq(6), always())
			.returning(|_, _, buf| {
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
		t.expect_write()
			.once()
			.in_sequence(&mut seq)
			.with(
				eq(page_id!(0x0, 0x1)),
				eq(6),
				eq([
					0x2001_u32.to_ne_bytes().as_slice(),
					0x1_u16.to_ne_bytes().as_slice(),
				]
				.concat()),
			)
			.returning(|_, _, _| Ok(()));

		// when
		let page_id = PageAllocator::alloc(&mut t).unwrap();

		// then
		assert_eq!(page_id, page_id!(0x2000, 0xffff));
	}
}
