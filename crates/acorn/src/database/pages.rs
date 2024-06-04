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
	type Error = String;

	fn try_from(value: PageIdRepr) -> Result<Self, String> {
		let Some(page_num) = NonZeroU16::new(value.page_num) else {
			return Err(String::from("Found invalid page number '0'!"));
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct MetaPage(PageId);

impl MetaPage {
	const FREELIST_HEAD_OFFSET: usize = 0;
	const NEXT_PAGE_ID_OFFSET: usize = Self::FREELIST_HEAD_OFFSET + size_of::<PageIdRepr>();

	pub const fn new(page_id: PageId) -> Self {
		Self(page_id)
	}

	#[inline]
	pub const fn page_id(self) -> PageId {
		self.0
	}

	pub fn get_freelist_head(
		self,
		reader: &impl ReadPage,
	) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(self.0, Self::FREELIST_HEAD_OFFSET, repr.as_bytes_mut())?;
		Ok(repr.into())
	}

	pub fn set_freelist_head(
		self,
		mut writer: impl WritePage,
		value: Option<PageId>,
	) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		writer.write(self.0, Self::FREELIST_HEAD_OFFSET, repr.as_bytes())?;
		Ok(())
	}

	pub fn get_next_page_id(self, reader: impl ReadPage) -> Result<PageId, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(self.0, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes_mut())?;

		repr.try_into()
			.map_err(|msg| DatabaseError::PageFormat(self.0, msg))
	}

	pub fn set_next_page_id(
		self,
		mut writer: impl WritePage,
		value: PageId,
	) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		writer.write(self.0, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes())?;
		Ok(())
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FreelistPage(PageId);

impl FreelistPage {
	const NEXT_PAGE_ID_OFFSET: usize = 0;
	const LENGTH_OFFSET: usize = Self::NEXT_PAGE_ID_OFFSET + size_of::<PageIdRepr>();
	const ITEMS_OFFSET: usize = Self::LENGTH_OFFSET + size_of::<u16>();

	pub const NUM_SLOTS: usize = (PAGE_BODY_SIZE - Self::ITEMS_OFFSET) / size_of::<PageIdRepr>();

	pub const fn new(page_id: PageId) -> Self {
		Self(page_id)
	}

	#[inline]
	pub const fn page_id(self) -> PageId {
		self.0
	}

	pub fn get_next_page_id(self, reader: &impl ReadPage) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(self.0, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes_mut())?;
		Ok(repr.into())
	}

	pub fn set_next_page_id(
		self,
		mut writer: impl WritePage,
		value: Option<PageId>,
	) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		writer.write(self.0, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes())?;
		Ok(())
	}

	pub fn get_length(self, reader: impl ReadPage) -> Result<usize, DatabaseError> {
		let mut repr = [0; 2];
		reader.read(self.0, Self::LENGTH_OFFSET, &mut repr)?;
		Ok(u16::from_ne_bytes(repr).into())
	}

	pub fn set_length(self, mut writer: impl WritePage, value: usize) -> Result<(), DatabaseError> {
		let repr = u16::try_from(value).expect("Freelist page length must be 16-bit!");
		writer.write(self.0, Self::LENGTH_OFFSET, &repr.to_ne_bytes())?;
		Ok(())
	}

	pub fn get_item(
		self,
		reader: impl ReadPage,
		index: usize,
	) -> Result<Option<PageId>, DatabaseError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(
			self.0,
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes_mut(),
		)?;
		Ok(repr.into())
	}

	pub fn set_item(
		self,
		mut writer: impl WritePage,
		index: usize,
		value: Option<PageId>,
	) -> Result<(), DatabaseError> {
		let repr = PageIdRepr::from(value);
		writer.write(
			self.0,
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes(),
		)?;
		Ok(())
	}
}
