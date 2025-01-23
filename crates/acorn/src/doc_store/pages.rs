use std::{
	mem::{self, offset_of},
	num::NonZeroU16,
};

use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes};

use crate::{
	files::segment::PAGE_BODY_SIZE,
	page_store::{PageId, ReadPage, WritePage},
	repr::Repr,
	utils::units::B,
};

use super::DatabaseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PageKind {
	FreelistMeta = 0,
	FreelistBlock = 1,
	Records = 2,
}

impl PageKind {
	fn from(value: u8) -> Option<Self> {
		match value {
			0 => Some(PageKind::FreelistMeta),
			1 => Some(PageKind::FreelistBlock),
			2 => Some(PageKind::Records),
			_ => None,
		}
	}
}

struct PageHeader {
	kind: PageKind,
}

#[derive(Debug, Immutable, IntoBytes, FromBytes)]
#[repr(C)]
struct PageHeaderRepr {
	kind: u8,
}

impl TryFrom<PageHeaderRepr> for PageHeader {
	type Error = DatabaseError;

	fn try_from(value: PageHeaderRepr) -> Result<Self, Self::Error> {
		let Some(kind) = PageKind::from(value.kind) else {
			return Err(DatabaseError::UnknownPageKind(value.kind));
		};
		Ok(Self { kind })
	}
}

impl From<PageHeader> for PageHeaderRepr {
	fn from(value: PageHeader) -> Self {
		Self {
			kind: value.kind as u8,
		}
	}
}

impl Repr<PageHeader> for PageHeaderRepr {
	type Error = DatabaseError;
}

#[derive(Debug, Immutable, IntoBytes, FromBytes)]
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

macro_rules! read_section {
	($page:expr, $page_repr:ident.$field:ident, $field_repr:ty) => {{
		let mut repr = <$field_repr as FromZeros>::new_zeroed();
		ReadPage::read(
			&$page,
			mem::offset_of!($page_repr, $field),
			repr.as_mut_bytes(),
		)
		.map_err(DatabaseError::from)
		.and_then(|_| repr.try_into().map_err(DatabaseError::from))
	}};
}

macro_rules! write_section {
	($page:expr, $page_repr:ident.$field:ident, $field_repr:ident, $value:expr) => {{
		let repr: $field_repr = $value.into();
		WritePage::write(
			&mut $page,
			mem::offset_of!($page_repr, $field),
			repr.as_bytes(),
		)
		.map_err(DatabaseError::from)
	}};
}

macro_rules! read_array_section {
	($page:expr, $page_repr:ident.$field:ident, $field_repr:ident, $index:expr) => {{
		let mut repr = <$field_repr as FromZeros>::new_zeroed();
		let index = mem::offset_of!($page_repr, $field) + mem::size_of::<$field_repr>() * $index;
		if index + mem::size_of::<$field_repr>() > PAGE_BODY_SIZE {
			Err(DatabaseError::PageIndexOutOfBounds)
		} else {
			ReadPage::read(
				&$page,
				mem::offset_of!($page_repr, $field) + mem::size_of::<$field_repr>() * $index,
				repr.as_mut_bytes(),
			)
			.map_err(DatabaseError::from)
			.and_then(|_| repr.try_into().map_err(DatabaseError::from))
		}
	}};
}

macro_rules! write_array_section {
	($page:expr, $page_repr:ident.$field:ident, $field_repr:ident, $index:expr, $value:expr) => {{
		let repr: $field_repr = $value.into();
		let index = mem::offset_of!($page_repr, $field) + mem::size_of::<$field_repr>() * $index;
		if index + mem::size_of::<$field_repr>() > PAGE_BODY_SIZE {
			Ok(())
		} else {
			WritePage::write(
				&mut $page,
				mem::offset_of!($page_repr, $field) + mem::size_of::<$field_repr>() * $index,
				repr.as_bytes(),
			)
			.map_err(DatabaseError::from)
		}
	}};
}

#[repr(C, packed)]
struct MetaPageFormat {
	header: PageHeaderRepr,
	freelist_head: PageIdRepr,
	next_page_id: PageIdRepr,
}

pub(super) struct MetaPage<P>(P);

impl<P> MetaPage<P> {
	pub fn new_unchecked(page: P) -> Self {
		Self(page)
	}
}

impl<P: ReadPage> MetaPage<P> {
	pub fn new(page: P) -> Result<Self, DatabaseError> {
		let header: PageHeader = read_section!(page, MetaPageFormat.header, PageHeaderRepr)?;
		if header.kind != PageKind::FreelistMeta {
			return Err(DatabaseError::UnexpectedPageKind {
				expected: PageKind::FreelistMeta,
				received: header.kind,
			});
		}
		Ok(Self::new_unchecked(page))
	}

	pub fn get_freelist_head(&self) -> Result<Option<PageId>, DatabaseError> {
		read_section!(self.0, MetaPageFormat.freelist_head, PageIdRepr)
	}

	pub fn get_next_page_id(&self) -> Result<PageId, DatabaseError> {
		read_section!(self.0, MetaPageFormat.next_page_id, PageIdRepr)
	}
}

impl<P: WritePage> MetaPage<P> {
	pub fn init(&mut self, next_page_id: PageId) -> Result<(), DatabaseError> {
		write_section!(
			self.0,
			MetaPageFormat.header,
			PageHeaderRepr,
			PageHeader {
				kind: PageKind::FreelistMeta
			}
		)?;
		self.set_freelist_head(None)?;
		self.set_next_page_id(next_page_id)?;
		Ok(())
	}

	pub fn set_freelist_head(&mut self, value: Option<PageId>) -> Result<(), DatabaseError> {
		write_section!(self.0, MetaPageFormat.freelist_head, PageIdRepr, value)
	}

	pub fn set_next_page_id(&mut self, value: PageId) -> Result<(), DatabaseError> {
		write_section!(self.0, MetaPageFormat.next_page_id, PageIdRepr, value)
	}
}

#[repr(C, packed)]
struct FreelistPageFormat {
	header: PageHeaderRepr,
	next_page_id: PageIdRepr,
	length: u16,
	items: [PageIdRepr; 0],
}

impl FreelistPageFormat {
	const NUM_SLOTS: usize =
		(PAGE_BODY_SIZE - offset_of!(FreelistPageFormat, items)) / size_of::<PageIdRepr>();
}

pub(super) struct FreelistPage<P>(P);

impl<P> FreelistPage<P> {
	pub const NUM_SLOTS: usize = FreelistPageFormat::NUM_SLOTS;

	pub fn new_unchecked(page: P) -> Self {
		Self(page)
	}
}

impl<P: ReadPage> FreelistPage<P> {
	pub fn new(page: P) -> Result<Self, DatabaseError> {
		let header: PageHeader = read_section!(page, FreelistPageFormat.header, PageHeaderRepr)?;
		if header.kind != PageKind::FreelistBlock {
			return Err(DatabaseError::UnexpectedPageKind {
				expected: PageKind::FreelistBlock,
				received: header.kind,
			});
		}
		Ok(Self::new_unchecked(page))
	}

	pub fn get_next_page_id(&self) -> Result<Option<PageId>, DatabaseError> {
		read_section!(self.0, FreelistPageFormat.next_page_id, PageIdRepr)
	}

	pub fn get_length(&self) -> Result<usize, DatabaseError> {
		read_section!(self.0, FreelistPageFormat.length, u16)
	}

	pub fn is_full(&self) -> Result<bool, DatabaseError> {
		Ok(self.get_length()? >= FreelistPageFormat::NUM_SLOTS)
	}

	pub fn get_item(&self, index: usize) -> Result<Option<PageId>, DatabaseError> {
		read_array_section!(self.0, FreelistPageFormat.items, PageIdRepr, index)
	}
}

impl<P: WritePage> FreelistPage<P> {
	pub fn init(&mut self) -> Result<(), DatabaseError> {
		write_section!(
			self.0,
			FreelistPageFormat.header,
			PageHeaderRepr,
			PageHeader {
				kind: PageKind::FreelistBlock
			}
		)?;
		self.set_next_page_id(None)?;
		self.set_length(0)?;
		Ok(())
	}

	pub fn set_next_page_id(&mut self, value: Option<PageId>) -> Result<(), DatabaseError> {
		write_section!(self.0, FreelistPageFormat.next_page_id, PageIdRepr, value)
	}

	fn set_length(&mut self, value: usize) -> Result<(), DatabaseError> {
		let repr = u16::try_from(value).expect("Freelist page length must be 16-bit!");
		write_section!(self.0, FreelistPageFormat.length, u16, repr)
	}

	fn set_item(&mut self, index: usize, value: Option<PageId>) -> Result<(), DatabaseError> {
		write_array_section!(self.0, FreelistPageFormat.items, PageIdRepr, index, value)
	}
}

impl<P: ReadPage + WritePage> FreelistPage<P> {
	pub fn push_item(&mut self, value: PageId) -> Result<(), DatabaseError> {
		let index = self.get_length()?;
		self.set_item(index, Some(value))?;
		self.set_length(index + 1)?;
		Ok(())
	}

	pub fn pop_item(&mut self) -> Result<Option<PageId>, DatabaseError> {
		let mut index = self.get_length()?;
		loop {
			if index == 0 {
				return Ok(None);
			}
			index -= 1;
			if let Some(item) = self.get_item(index)? {
				self.set_length(index)?;
				return Ok(Some(item));
			}
		}
	}
}
