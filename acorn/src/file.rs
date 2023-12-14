use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use memmap::MmapMut;

pub struct StorageFile {
    file: File,
    mmap: MmapMut,
}

impl StorageFile {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        todo!();
    }
}
