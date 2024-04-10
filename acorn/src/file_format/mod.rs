pub(crate) mod wal;

pub(crate) struct GenericHeader {
	magic: [u8; 4],
	file_type: u8,
}
