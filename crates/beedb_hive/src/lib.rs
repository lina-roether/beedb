// Lint config
#![allow(dead_code)] // TODO: temporary
#![cfg_attr(
	not(test),
	warn(clippy::cast_possible_wrap),
	warn(clippy::cast_possible_truncation)
)]
// Unstable features
#![cfg_attr(test, feature(test))]

#[cfg(test)]
extern crate test;

mod consts;
mod doc_store;
mod files;
mod page_store;
mod repr;
mod tasks;
mod utils;
