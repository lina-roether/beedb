use std::{
	io::{Read, Write},
	mem::size_of,
};

use crc::Crc;
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::FileError;

// TODO: there are tradeoffs here. Perhaps I should look more into selecting an
// algorithm.
pub(crate) const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

pub(crate) trait Serialized: Sized
where
	FileError: From<<Self::Repr as TryInto<Self>>::Error>,
{
	type Repr: Clone + AsBytes + FromBytes + FromZeroes + From<Self> + TryInto<Self>;

	const REPR_SIZE: usize = size_of::<Self::Repr>();

	fn serialize(self, mut writer: impl Write) -> Result<(), FileError> {
		let repr = Self::Repr::from(self);
		writer.write_all(repr.as_bytes())?;
		Ok(())
	}

	fn deserialize(mut reader: impl Read) -> Result<Self, FileError> {
		let mut repr = Self::Repr::new_zeroed();
		reader.read_exact(repr.as_bytes_mut())?;
		let value: Self = repr.try_into()?;
		Ok(value)
	}

	fn from_repr_bytes(bytes: &[u8]) -> Result<Self, FileError> {
		let Some(repr) = Self::Repr::ref_from_prefix(bytes) else {
			return Err(FileError::UnexpectedEof);
		};
		let value: Self = repr.clone().try_into()?;
		Ok(value)
	}

	fn write_repr_bytes(self, bytes: &mut [u8]) {
		let repr = Self::Repr::from(self);
		bytes[0..Self::REPR_SIZE].copy_from_slice(repr.as_bytes());
	}
}
