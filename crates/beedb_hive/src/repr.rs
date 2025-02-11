use std::{
	io::{self, Read, Write},
	mem,
};

use zerocopy::{FromBytes, Immutable, IntoBytes};

pub(crate) trait Repr<T>: Sized + FromBytes + IntoBytes
where
	T: TryFrom<Self> + Into<Self>,
{
	type Error: From<T::Error>;

	const SIZE: usize = mem::size_of::<Self>();

	fn from_bytes(bytes: &[u8]) -> Result<T, Self::Error> {
		let mut repr = Self::new_zeroed();
		repr.as_mut_bytes().copy_from_slice(bytes);
		Ok(T::try_from(repr)?)
	}
}

pub(crate) trait IoRepr<T>: Repr<T>
where
	T: TryFrom<Self> + Into<Self>,
	Self: Immutable,
	Self::Error: From<io::Error>,
{
	fn serialize(value: T, mut writer: impl Write) -> Result<(), Self::Error> {
		let repr: Self = value.into();
		writer.write_all(repr.as_bytes())?;
		Ok(())
	}

	fn deserialize(mut reader: impl Read) -> Result<T, Self::Error> {
		let mut repr = Self::new_zeroed();
		reader.read_exact(repr.as_mut_bytes())?;
		Ok(T::try_from(repr)?)
	}
}

impl<T, R> IoRepr<T> for R
where
	R: Repr<T>,
	T: TryFrom<R> + Into<R>,
	Self: Immutable,
	Self::Error: From<io::Error>,
{
}
