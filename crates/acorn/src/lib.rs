// Lint config
#![allow(dead_code)] // TODO: temporary
#![cfg_attr(
	not(test),
	warn(clippy::cast_possible_wrap),
	warn(clippy::cast_possible_truncation)
)]
// Unstable features
#![feature(trait_alias)]
#![feature(duration_constructors)]
#![feature(async_closure)]
#![feature(buf_read_has_data_left)]
#![feature(cfg_match)]
#![feature(os_str_display)]
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
