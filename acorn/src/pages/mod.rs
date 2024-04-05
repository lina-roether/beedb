mod freelist;
mod header;

use std::{borrow::Cow, ops::Range};

pub(crate) use freelist::*;
pub(crate) use header::*;

#[derive(Debug)]
pub(crate) struct WriteOp<'a> {
	pub start: usize,
	pub bytes: Cow<'a, [u8]>,
}

impl<'a> Clone for WriteOp<'a> {
	fn clone(&self) -> Self {
		*self
	}
}

impl<'a> WriteOp<'a> {
	pub fn new(start: usize, bytes: impl Into<Cow<'a, [u8]>>) -> Self {
		Self {
			start,
			bytes: bytes.into(),
		}
	}

	#[inline]
	pub fn range(&self) -> Range<usize> {
		self.start..(self.start + self.bytes.len())
	}
}

#[derive(Debug)]
pub(crate) struct ReadOp<'a> {
	pub start: usize,
	pub bytes: &'a mut [u8],
}

impl<'a> ReadOp<'a> {
	pub fn new(start: usize, bytes: &'a mut [u8]) -> Self {
		Self { start, bytes }
	}

	#[inline]
	pub fn range(&self) -> Range<usize> {
		self.start..(self.start + self.bytes.len())
	}
}
