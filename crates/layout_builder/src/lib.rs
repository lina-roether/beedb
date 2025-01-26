pub trait Read {
	type Error;

	fn read(&self, offset: usize, buf: &mut [u8]) -> Result<(), Self::Error>;
}

pub trait Write {
	type Error;

	fn write(&mut self, offset: usize, buf: &[u8]) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LayoutSize {
	Fixed(isize),
	GrowForwards,
	GrowBackwards,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Block {
	pub length: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LayoutSegment {
	offset: usize,
	layout: Layout,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Layout {
	Block(Block),
}
