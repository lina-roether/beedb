#![feature(alloc_layout_extra)]
#![feature(const_alloc_layout)]
#![feature(assert_matches)]
#![feature(cfg_match)]
#![feature(ptr_metadata)]
#![feature(pointer_is_aligned)]
#![feature(test)]
#![allow(dead_code)]

#[cfg(test)]
extern crate test;

mod cache;
mod consts;
mod disk;
mod index;
mod io;
mod manage;
mod pages;
mod utils;
mod wal;

