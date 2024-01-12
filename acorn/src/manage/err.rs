use thiserror::Error;

use crate::disk;

#[derive(Debug, Error)]
pub enum Error {
	#[error("Transaction with id {0} doesn't exist")]
	NoSuchTransaction(u64),

	#[error("Segment {0} is corrupted")]
	CorruptedSegment(u32),

	#[error("You've somehow reached acorn's internal size limit limit, which is 4 exibytes, assuming you're using the default page size. Great job! Your database is now broken. ¯\\_(ツ)_/¯")]
	SizeLimitReached,

	#[error(transparent)]
	Disk(#[from] disk::Error),
}
