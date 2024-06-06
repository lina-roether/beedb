use std::{mem::size_of, num::NonZeroU16};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{
	files::segment::PAGE_BODY_SIZE,
	storage::{PageId, ReadPage, WritePage},
};

use super::DatabaseError;

#[derive(AsBytes, FromZeroes, FromBytes)]
#[repr(C, packed)]
struct PageIdRepr {
	segment_num: u32,
	page_num: u16,
}

impl TryFrom<PageIdRepr> for PageId {
	type Error = DatabaseError;

	fn try_from(value: PageIdRepr) -> Result<Self, DatabaseError> {
		let Some(page_num) = NonZeroU16::new(value.page_num) else {
			return Err(DatabaseError::PageFormat(
				"Found invalid page number '0'!".to_string(),
			));
		};
		Ok(PageId::new(value.segment_num, page_num))
	}
}

impl From<PageId> for PageIdRepr {
	fn from(value: PageId) -> Self {
		Self {
			segment_num: value.segment_num,
			page_num: value.page_num.get(),
		}
	}
}

impl From<PageIdRepr> for Option<PageId> {
	fn from(value: PageIdRepr) -> Self {
		Some(PageId::new(
			value.segment_num,
			NonZeroU16::new(value.page_num)?,
		))
	}
}

impl From<Option<PageId>> for PageIdRepr {
	fn from(value: Option<PageId>) -> Self {
		match value {
			Some(page_id) => page_id.into(),
			None => PageIdRepr::new_zeroed(),
		}
	}
}

pub(crate) struct MetaPage<P>(P);

impl<P> MetaPage<P> {
	const FREELIST_HEAD_OFFSET: usize = 0;
	const NEXT_PAGE_ID_OFFSET: usize = Self::FREELIST_HEAD_OFFSET + size_of::<PageIdRepr>();
}

impl<P: ReadPage> MetaPage<P> {
	pub fn new(page: P) -> Self {
		Self(page)
	}

	pub fn get_freelist_head(&self) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		self.0
			.read(Self::FREELIST_HEAD_OFFSET, repr.as_bytes_mut())?;
		Ok(repr.into())
	}

	pub fn get_next_page_id(&self) -> Result<PageId, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		self.0
			.read(Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes_mut())?;
		repr.try_into()
	}
}

impl<P: WritePage> MetaPage<P> {
	pub fn new_mut(page: P) -> Self {
		Self(page)
	}

	pub fn set_freelist_head(&mut self, value: Option<PageId>) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		self.0.write(Self::FREELIST_HEAD_OFFSET, repr.as_bytes())?;
		Ok(())
	}

	pub fn set_next_page_id(&mut self, value: PageId) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		self.0.write(Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes())?;
		Ok(())
	}
}

pub(crate) struct FreelistPage<P>(P);

impl<P> FreelistPage<P> {
	const NEXT_PAGE_ID_OFFSET: usize = 0;
	const LENGTH_OFFSET: usize = Self::NEXT_PAGE_ID_OFFSET + size_of::<PageIdRepr>();
	const ITEMS_OFFSET: usize = Self::LENGTH_OFFSET + size_of::<u16>();

	pub const NUM_SLOTS: usize = (PAGE_BODY_SIZE - Self::ITEMS_OFFSET) / size_of::<PageIdRepr>();
}

impl<P: ReadPage> FreelistPage<P> {
	pub fn new(page: P) -> Self {
		Self(page)
	}

	pub fn get_next_page_id(&self) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		self.0
			.read(Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes_mut())?;
		Ok(repr.into())
	}

	pub fn get_length(&self) -> Result<usize, DatabaseError> {
		let mut repr = [0; 2];
		self.0.read(Self::LENGTH_OFFSET, &mut repr)?;
		Ok(u16::from_ne_bytes(repr).into())
	}

	pub fn get_item(&self, index: usize) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		self.0.read(
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes_mut(),
		)?;
		Ok(repr.into())
	}
}

impl<P: WritePage> FreelistPage<P> {
	pub fn set_next_page_id(&mut self, value: Option<PageId>) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		self.0.write(Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes())?;
		Ok(())
	}

	pub fn set_length(&mut self, value: usize) -> Result<(), DatabaseError> {
		let repr = u16::try_from(value).expect("Freelist page length must be 16-bit!");
		self.0.write(Self::LENGTH_OFFSET, &repr.to_ne_bytes())?;
		Ok(())
	}

	pub fn set_item(&mut self, index: usize, value: Option<PageId>) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		self.0.write(
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes(),
		)?;
		Ok(())
	}
}
