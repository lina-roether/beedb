use thiserror::Error;

use crate::disk;

#[derive(Debug, Error)]
pub enum Error {
	#[error("Transaction with id {0} doesn't exist")]
	NoSuchTransaction(u64),

	#[error("Segment {0} is corrupted")]
	CorruptedSegment(u32),

	#[error(transparent)]
	Disk(#[from] disk::Error),
}
