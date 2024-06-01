use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::consts::SMALL_STRING_SIZE;

use super::DatabaseError;

#[derive(Debug, FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
pub(super) struct SmallStringRepr([u8; SMALL_STRING_SIZE]);

impl SmallStringRepr {
	fn from(value: String) -> Option<Self> {
		if value.bytes().len() >= 8 || value.contains("\0") {
			return None;
		}
		let str_bytes = value.as_bytes();
		let mut bytes = [0; SMALL_STRING_SIZE];
		bytes[0..str_bytes.len()].copy_from_slice(str_bytes);
		Some(SmallStringRepr(bytes))
	}
}

impl TryInto<String> for SmallStringRepr {
	type Error = DatabaseError;

	fn try_into(self) -> Result<String, Self::Error> {
		let mut str = String::from_utf8(self.0.to_vec())?;
		let len = str.find("\0").unwrap_or(str.len());
		str.truncate(len);
		Ok(str)
	}
}
