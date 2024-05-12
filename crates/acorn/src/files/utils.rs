use std::{
	io::{Read, Write},
	mem::size_of,
};

use crc::Crc;
use zerocopy::{AsBytes, FromBytes};

use super::FileError;

// TODO: there are tradeoffs here. Perhaps I should look more into selecting an
// algorithm.
pub(crate) const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
pub(crate) const CRC16: Crc<u16> = Crc::<u16>::new(&crc::CRC_16_IBM_SDLC);

pub(crate) trait Repr<T>: Sized + FromBytes + AsBytes
where
	T: TryFrom<Self> + Into<Self>,
	FileError: From<T::Error>,
{
	const SIZE: usize = size_of::<Self>();

	fn serialize(value: T, mut writer: impl Write) -> Result<(), FileError> {
		let repr: Self = value.into();
		writer.write_all(repr.as_bytes())?;
		Ok(())
	}

	fn deserialize(mut reader: impl Read) -> Result<T, FileError> {
		let mut repr = Self::new_zeroed();
		reader.read_exact(repr.as_bytes_mut())?;
		Ok(T::try_from(repr)?)
	}

	fn from_bytes(bytes: &[u8]) -> Result<T, FileError> {
		let mut repr = Self::new_zeroed();
		repr.as_bytes_mut().copy_from_slice(bytes);
		Ok(T::try_from(repr)?)
	}
}
