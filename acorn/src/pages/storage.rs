pub trait StorageLocation {
    fn new_page(&mut self) -> usize;

    fn delete_page(&mut self, id: usize);

    fn read_page(&mut self, id: usize, buf: &mut [u8]);

    fn write_page(&mut self, id: usize, buf: &[u8]);
}

pub struct TestStorage<const PAGE_SIZE: usize> {
    pages: Vec<[u8; PAGE_SIZE]>,
}

impl<const PAGE_SIZE: usize> StorageLocation for TestStorage<PAGE_SIZE> {
    fn new_page(&mut self) -> usize {
        self.pages.len()
    }

    fn delete_page(&mut self, _id: usize) {}

    fn read_page(&mut self, id: usize, buf: &mut [u8]) {
        buf.clone_from_slice(&self.pages[id])
    }

    fn write_page(&mut self, id: usize, buf: &[u8]) {
        self.pages[id].clone_from_slice(buf);
    }
}
