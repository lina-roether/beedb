use std::{num::NonZeroU32, usize};

use parking_lot::RwLock;
use thiserror::Error;

use crate::utils::{
	byte_order::ByteOrder,
	units::{KiB, B},
};

use self::{
	file::StorageFile,
	format::{Meta, PageStorage, State},
};

mod file;
mod format;

const CURRENT_VERSION: u8 = 1;
const MIN_PAGE_SIZE: usize = 512 * B;
const DEFAULT_PAGE_SIZE: usize = 8 * KiB;

#[derive(Debug, Error)]
pub enum LoadError {
	#[error(
		"The storage file has an unsupported version (v{0}); Only v{CURRENT_VERSION} is supported"
	)]
	UnsupportedVersion(u8),

	#[error("The page size set in the storage file ({0}) is invalid")]
	InvalidPageSize(usize),

	#[error("Cannot open a {0} storage file on a {} system", ByteOrder::NATIVE)]
	ByteOrderMismatch(ByteOrder),

	#[error(transparent)]
	Format(#[from] format::Error),
}

#[derive(Debug, Error)]
pub enum InitError {
	#[error("Invalid page size {0}; must be a power of two, and at least {MIN_PAGE_SIZE}")]
	InvalidPageSize(usize),

	#[error(transparent)]
	Format(#[from] format::Error),
}

#[derive(Debug, Error)]
pub enum AccessError {
	#[error(transparent)]
	Format(#[from] format::Error),
}

#[derive(Debug, Clone)]
pub struct InitParams {
	pub page_size: usize,
}

impl Default for InitParams {
	fn default() -> Self {
		Self {
			page_size: DEFAULT_PAGE_SIZE,
		}
	}
}

pub struct Storage<F: StorageFile> {
	meta: Meta,
	state: RwLock<State>,
	pages: PageStorage<F>,
	current_freelist_page: Vec<NonZeroU32>,
}

impl<F: StorageFile> Storage<F> {
	pub fn load(file: F) -> Result<Self, LoadError> {
		let meta = Meta::read_from(&file)?;

		if meta.format_version != CURRENT_VERSION {
			return Err(LoadError::UnsupportedVersion(meta.format_version));
		}

		if meta.byte_order != ByteOrder::NATIVE {
			return Err(LoadError::ByteOrderMismatch(meta.byte_order));
		}

		let page_size = 1 << meta.page_size_exponent;
		if page_size < MIN_PAGE_SIZE {
			return Err(LoadError::InvalidPageSize(page_size));
		}

		let state = State::read_from(&file)?;
		let pages = PageStorage::new(file, page_size);

		Ok(Self::new(meta, state, pages))
	}

	pub fn init(mut file: F, params: InitParams) -> Result<Self, InitError> {
		if params.page_size < MIN_PAGE_SIZE || !params.page_size.is_power_of_two() {
			return Err(InitError::InvalidPageSize(params.page_size));
		}

		let meta = Meta {
			format_version: CURRENT_VERSION,
			page_size_exponent: params.page_size.ilog2() as u8,
			byte_order: ByteOrder::NATIVE,
		};
		meta.write_to(&mut file)?;

		let state = State {
			num_pages: 0,
			freelist_length: 0,
			freelist_trunk: None,
		};
		state.write_to(&mut file)?;

		let pages = PageStorage::new(file, params.page_size);

		Ok(Self::new(meta, state, pages))
	}

	#[inline]
	pub fn read_page(&self, buf: &mut [u8], page_number: NonZeroU32) -> Result<(), AccessError> {
		self.pages.read_page(buf, page_number)?;
		Ok(())
	}

	#[inline]
	pub fn write_page(&mut self, buf: &[u8], page_number: NonZeroU32) -> Result<(), AccessError> {
		self.pages.write_page(buf, page_number)?;
		Ok(())
	}

	#[inline]
	pub fn page_size(&self) -> usize {
		self.pages.page_size()
	}

	#[inline]
	pub fn page_size_exponent(&self) -> u8 {
		self.meta.page_size_exponent
	}

	fn new(meta: Meta, state: State, pages: PageStorage<F>) -> Self {
		Self {
			meta,
			state: RwLock::new(state),
			pages,
			current_freelist_page: Vec::new(),
		}
	}
}
