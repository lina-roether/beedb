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
pub(crate) const CRC16: Crc<u16> = Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

pub(crate) trait Serialized: Sized
where
	FileError: From<<Self::Repr as TryInto<Self>>::Error>,
{
	type Repr: Clone + AsBytes + FromBytes + FromZeroes + From<Self> + TryInto<Self>;

	const REPR_SIZE: usize = size_of::<Self::Repr>();

	fn serialize(self, mut writer: impl Write) -> Result<(), FileError> {
		writer.write_all(self.into_repr().as_bytes())?;
		Ok(())
	}

	fn deserialize(mut reader: impl Read) -> Result<Self, FileError> {
		let mut repr = Self::Repr::new_zeroed();
		reader.read_exact(repr.as_bytes_mut())?;
		Ok(repr.try_into()?)
	}

	fn into_repr(self) -> Self::Repr {
		Self::Repr::from(self)
	}

	fn from_repr_bytes(bytes: &[u8]) -> Result<Self, FileError> {
		let mut repr = Self::Repr::new_zeroed();
		repr.as_bytes_mut().copy_from_slice(bytes);
		Ok(repr.try_into()?)
	}
}
