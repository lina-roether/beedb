use std::io;

use thiserror::Error;

use crate::disk;

#[derive(Debug, Error)]
pub enum Error {
	#[error("Segment {0} is corrupted")]
	CorruptedSegment(u32),

	#[error("You've somehow reached acorn's internal size limit limit, which is 4 exibytes, assuming you're using the default page size. Great job! Your database is now broken. ¯\\_(ツ)_/¯")]
	SizeLimitReached,

	#[error(transparent)]
	Disk(#[from] disk::Error),

	#[error("Failed to read from WAL: {0}")]
	WalRead(io::Error),

	#[error("Failed to write to WAL: {0}")]
	WalWrite(io::Error),
}
