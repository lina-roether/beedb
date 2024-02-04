mod b_tree;
mod freelist;
mod header;

use std::ops::Range;

pub(crate) use b_tree::*;
pub(crate) use freelist::*;
pub(crate) use header::*;

#[derive(Debug)]
pub(crate) struct WriteOp<'a> {
	pub start: usize,
	pub bytes: &'a [u8],
}

impl<'a> Clone for WriteOp<'a> {
	fn clone(&self) -> Self {
		*self
	}
}

impl<'a> Copy for WriteOp<'a> {}

impl<'a> WriteOp<'a> {
	pub fn new(start: usize, bytes: &'a [u8]) -> Self {
		Self { start, bytes }
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
