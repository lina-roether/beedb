use std::ops::{Deref, DerefMut};

use memmap::MmapMut;

trait Buffer = Deref<Target = [u8]> + DerefMut<Target = [u8]>;

struct SegmentFile<F: Buffer = MmapMut> {
	buffer: F,
}

impl<F: Buffer> SegmentFile<F> {
	fn new(buffer: F) -> Self {
		Self { buffer }
	}
}
