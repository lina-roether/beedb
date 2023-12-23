/*
 * State block layout:
 *
 * | Offset | Size | Description                                                                |
 * |--------|------|----------------------------------------------------------------------------|
 * |      0 |    4 | The current size of the dababase in pages.                                 |
 * |      4 |    4 | The first page of the freelist.                                            |
 * |      8 |    4 | The current length of the freelist.                                        |
 * |     12 |   20 | Reserved for future use. Must be zero.                                     |
 *
 */

use std::{num::NonZeroU32, usize};

use crate::storage::StorageFile;

use super::Error;

#[derive(Debug)]
pub struct State {
	pub num_pages: usize,
	pub freelist_trunk: Option<NonZeroU32>,
	pub freelist_length: usize,
}

impl State {
	const SIZE: usize = 32;

	pub fn read_from<const OFFSET: u64>(file: &impl StorageFile) -> Result<Self, Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();
		if file.read_at(&mut buf, OFFSET)? != buf.len() {
			return Err(Error::UnexpectedEOF);
		}

		let num_pages = u32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize;
		let freelist_trunk = NonZeroU32::new(u32::from_ne_bytes(buf[4..8].try_into().unwrap()));
		let freelist_length = u32::from_ne_bytes(buf[8..12].try_into().unwrap()) as usize;

		Ok(Self {
			num_pages,
			freelist_trunk,
			freelist_length,
		})
	}

	pub fn write_to<const OFFSET: u64>(&self, file: &mut impl StorageFile) -> Result<(), Error> {
		let mut buf: [u8; Self::SIZE] = Default::default();

		buf[0..4].copy_from_slice(&(self.num_pages as u32).to_ne_bytes());
		buf[4..8].copy_from_slice(
			&self
				.freelist_trunk
				.map(NonZeroU32::get)
				.unwrap_or(0)
				.to_ne_bytes(),
		);
		buf[8..12].copy_from_slice(&(self.freelist_length as u32).to_ne_bytes());

		if file.write_at(&buf, OFFSET)? != buf.len() {
			return Err(Error::IncompleteWrite);
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{assert_matches::assert_matches, iter};

	use super::*;

	#[test]
	fn read_state() {
		let mut data: Vec<u8> = vec![];
		data.extend(69_u32.to_ne_bytes());
		data.extend(420_u32.to_ne_bytes());
		data.extend(42069_u32.to_ne_bytes());
		data.extend(iter::repeat(0x00).take(20));

		let state = State::read_from::<0>(&data).unwrap();

		assert_eq!(state.num_pages, 69);
		assert_eq!(state.freelist_trunk, NonZeroU32::new(420));
		assert_eq!(state.freelist_length, 42069);
	}

	#[test]
	fn try_read_incomplete_state() {
		let data = vec![0x00, 0x00, 0x00];

		assert_matches!(State::read_from::<0>(&data), Err(Error::UnexpectedEOF));
	}

	#[test]
	fn write_state() {
		let mut data = Vec::<u8>::new();

		let state = State {
			num_pages: 123,
			freelist_trunk: NonZeroU32::new(543),
			freelist_length: 5432,
		};
		state.write_to::<0>(&mut data).unwrap();

		let mut expected: Vec<u8> = vec![];
		expected.extend(123_u32.to_ne_bytes());
		expected.extend(543_u32.to_ne_bytes());
		expected.extend(5432_u32.to_ne_bytes());
		expected.extend(iter::repeat(0x00).take(20));

		assert_eq!(data, expected);
	}
}
