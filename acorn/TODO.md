# TODO

- Move Wal into crate::disk::wal, and DiskStorage into crate::disk::storage
- Unify the different defragmented error enums (Maybe a single disk::Error?)
- What really needs to be an Arc<T>, and what doesn't?
- Implement warning for incomplete byte coverage for ByteView?
