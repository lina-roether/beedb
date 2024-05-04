use std::{collections::HashMap, sync::Arc};

use crate::files::{DatabaseFolder, DatabaseFolderApi};

pub(super) struct PhysicalStorage<DF = DatabaseFolder>
where
	DF: DatabaseFolderApi,
{
	folder: Arc<DF>,
	open_segments: HashMap<u32, DF::SegmentFile>,
}
