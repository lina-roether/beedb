use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ByteOrder {
	Big = 0,
	Little = 1,
}

impl ByteOrder {
	cfg_match! {
		cfg(target_endian = "big") => {
			pub const NATIVE: Self = Self::Big;
		}
		_ => {
			pub const NATIVE: Self = Self::Little;
		}
	}

	pub fn from_byte(byte: u8) -> Option<Self> {
		match byte {
			0 => Some(Self::Big),
			1 => Some(Self::Little),
			_ => None,
		}
	}
}

impl fmt::Display for ByteOrder {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Big => write!(f, "big-endian"),
			Self::Little => write!(f, "little-endian"),
		}
	}
}
