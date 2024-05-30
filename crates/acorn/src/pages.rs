use std::{mem::size_of, num::NonZeroU16};

use thiserror::Error;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{
	files::segment::PAGE_BODY_SIZE,
	storage::{PageId, ReadPage, StorageError, WritePage},
};

#[derive(Debug, Error)]
#[error("Page format error on page {page_id}: {message}")]
pub(crate) struct PageFormatError {
	page_id: PageId,
	message: String,
}

#[derive(Debug, Error)]
pub(crate) enum PageError {
	#[error(transparent)]
	Storage(#[from] StorageError),

	#[error(transparent)]
	Format(#[from] PageFormatError),
}

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

pub(crate) struct MetaPage;

impl MetaPage {
	const FREELIST_HEAD_OFFSET: usize = 0;

	pub fn read_freelist_head(
		page_id: PageId,
		reader: &impl ReadPage,
	) -> Result<PageId, PageError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(page_id, Self::FREELIST_HEAD_OFFSET, repr.as_bytes_mut())?;

		PageId::try_from(repr).map_err(|message| PageFormatError { page_id, message }.into())
	}

	pub fn write_freelist_head(
		page_id: PageId,
		writer: &mut impl WritePage,
		value: PageId,
	) -> Result<(), PageError> {
		let repr = PageIdRepr::from(value);
		writer.write(page_id, Self::FREELIST_HEAD_OFFSET, repr.as_bytes())?;
		Ok(())
	}
}

pub(crate) struct FreelistPage;

impl FreelistPage {
	const NEXT_PAGE_ID_OFFSET: usize = 0;
	const LENGTH_OFFSET: usize = Self::NEXT_PAGE_ID_OFFSET + size_of::<PageIdRepr>();
	const ITEMS_OFFSET: usize = Self::LENGTH_OFFSET + size_of::<u16>();

	pub const NUM_ITEMS: usize = (PAGE_BODY_SIZE - Self::ITEMS_OFFSET) / size_of::<PageIdRepr>();

	pub fn read_next_page_id(
		page_id: PageId,
		reader: &impl ReadPage,
	) -> Result<Option<PageId>, PageError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(page_id, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes_mut())?;
		Ok(repr.into())
	}

	pub fn write_next_page_id(
		page_id: PageId,
		writer: &mut impl WritePage,
		value: Option<PageId>,
	) -> Result<(), PageError> {
		let repr = PageIdRepr::from(value);
		writer.write(page_id, Self::NEXT_PAGE_ID_OFFSET, repr.as_bytes())?;
		Ok(())
	}

	pub fn read_length(page_id: PageId, reader: &impl ReadPage) -> Result<usize, PageError> {
		let mut repr = [0; 2];
		reader.read(page_id, Self::LENGTH_OFFSET, &mut repr)?;
		Ok(u16::from_ne_bytes(repr).into())
	}

	pub fn write_length(
		page_id: PageId,
		writer: &mut impl WritePage,
		value: usize,
	) -> Result<(), PageError> {
		let repr = u16::try_from(value).expect("Freelist page length must be 16-bit!");
		writer.write(page_id, Self::LENGTH_OFFSET, &repr.to_ne_bytes())?;
		Ok(())
	}

	pub fn read_item(
		page_id: PageId,
		reader: &impl ReadPage,
		index: usize,
	) -> Result<Option<PageId>, PageError> {
		let mut repr = PageIdRepr::new_zeroed();
		reader.read(
			page_id,
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes_mut(),
		)?;
		Ok(repr.into())
	}

	pub fn write_item(
		page_id: PageId,
		writer: &mut impl WritePage,
		index: usize,
		value: Option<PageId>,
	) -> Result<(), PageError> {
		let repr = PageIdRepr::from(value);
		writer.write(
			page_id,
			Self::ITEMS_OFFSET + index * size_of::<PageIdRepr>(),
			repr.as_bytes(),
		)?;
		Ok(())
	}
}
