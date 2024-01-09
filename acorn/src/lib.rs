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
mod utils;
