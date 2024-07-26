use std::{convert::Infallible, num::NonZero, string::FromUtf8Error};

use document::SchemaError;
use pages::PageKind;
use thiserror::Error;

use crate::page_store::{PageId, StorageError};

mod document;
mod document_repr;
mod page_alloc;
mod pages;

#[derive(Debug, Error)]
pub(crate) enum DatabaseError {
	#[error("Page format error: {0}")]
	PageFormat(String),

	#[error("Expected a page of kind {expected:?}, but received {received:?}. This usually indicates database corruption.")]
	UnexpectedPageKind {
		expected: PageKind,
		received: PageKind,
	},

	#[error("Received unknown page kind {0}. This may mean acorn is out of date.")]
	UnknownPageKind(u8),

	#[error("Tried to insert data to a page out of bounds")]
	PageIndexOutOfBounds,

	#[error(transparent)]
	StringEncoding(#[from] FromUtf8Error),

	#[error(transparent)]
	Schema(#[from] SchemaError),

	#[error(transparent)]
	Storage(#[from] StorageError),
}

impl From<Infallible> for DatabaseError {
	fn from(value: Infallible) -> Self {
		match value {}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DbPointer {
	segment_num: u32,
	page_num: NonZero<u16>,
	index: u16,
}

impl DbPointer {
	pub fn new(page_id: PageId, index: u16) -> Self {
		Self {
			segment_num: page_id.segment_num,
			page_num: page_id.page_num,
			index,
		}
	}

	#[inline]
	pub fn page_id(self) -> PageId {
		PageId::new(self.segment_num, self.page_num)
	}

	#[inline]
	pub fn index(self) -> u16 {
		self.index
	}
}
