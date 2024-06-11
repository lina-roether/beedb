use std::{num::NonZero, string::FromUtf8Error};

use document::SchemaError;
use thiserror::Error;

use crate::storage::{PageId, StorageError};

mod alloc;
mod document;
mod document_repr;
mod pages;

#[derive(Debug, Error)]
pub(crate) enum DatabaseError {
	#[error("Page format error: {0}")]
	PageFormat(String),

	#[error(transparent)]
	StringEncoding(#[from] FromUtf8Error),

	#[error(transparent)]
	Schema(#[from] SchemaError),

	#[error(transparent)]
	Storage(#[from] StorageError),
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
