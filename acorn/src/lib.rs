#![feature(alloc_layout_extra)]
#![feature(const_alloc_layout)]
#![feature(assert_matches)]
#![feature(cfg_match)]
#![feature(ptr_metadata)]
#![feature(pointer_is_aligned)]
#![allow(dead_code)]

mod cache;
mod consts;
mod disk;
mod index;
mod io;
mod manage;
mod pages;
mod transaction;
mod utils;

// Old stuff that is going to be removed int the new arch
#[cfg(old_stuff)]
mod segment;
#[cfg(old_stuff)]
mod storage;
