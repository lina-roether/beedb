use std::{convert::Infallible, mem, num::NonZero};

use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{page_store::PageId, repr::Repr};

use super::{DatabaseError, DbPointer};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
struct DbPointerRepr {
	segment_num: u32,
	page_num: u16,
	index: u16,
}

impl From<Option<DbPointer>> for DbPointerRepr {
	fn from(value: Option<DbPointer>) -> Self {
		let Some(value) = value else {
			return Self::new_zeroed();
		};
		Self {
			segment_num: value.segment_num,
			page_num: value.page_num.get(),
			index: value.index,
		}
	}
}

impl From<DbPointerRepr> for Option<DbPointer> {
	fn from(value: DbPointerRepr) -> Self {
		let page_num = NonZero::new(value.page_num)?;
		Some(DbPointer::new(
			PageId::new(value.segment_num, page_num),
			value.index,
		))
	}
}

impl From<DbPointer> for DbPointerRepr {
	fn from(value: DbPointer) -> Self {
		Self::from(Some(value))
	}
}

impl TryFrom<DbPointerRepr> for DbPointer {
	type Error = DatabaseError;

	fn try_from(value: DbPointerRepr) -> Result<Self, Self::Error> {
		let Some(pointer) = value.into() else {
			return Err(DatabaseError::PageFormat(
				"DB pointer was null!".to_string(),
			));
		};
		Ok(pointer)
	}
}

impl Repr<DbPointer> for DbPointerRepr {
	type Error = DatabaseError;
}

impl Repr<Option<DbPointer>> for DbPointerRepr {
	type Error = Infallible;
}

pub(crate) enum StoredValue {
	String(DbPointer),
	Bool(bool),
	Int(i64),
	Uint(u64),
	Float(f64),
}

impl StoredValue {
	pub fn size(&self) -> usize {
		match self {
			Self::String(..) => mem::size_of::<DbPointerRepr>(),
			Self::Bool(..) => mem::size_of::<bool>(),
			Self::Int(..) => mem::size_of::<i64>(),
			Self::Uint(..) => mem::size_of::<u64>(),
			Self::Float(..) => mem::size_of::<f64>(),
		}
	}
}

pub(crate) enum StoredDocument {
	Value(StoredValue),
	Option(Option<Box<StoredDocument>>),
	List(DbPointer),
	StuctFields(Vec<StoredDocument>),
}
