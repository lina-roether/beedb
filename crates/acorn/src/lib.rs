// Lint config
#![allow(dead_code)] // TODO: temporary
#![cfg_attr(not(test), warn(clippy::cast_possible_wrap))]
#![cfg_attr(not(test), warn(clippy::cast_possible_truncation))]
// Unstable features
#![feature(buf_read_has_data_left)]
#![feature(cfg_match)]
#![feature(os_str_display)]

mod consts;
mod files;
mod storage;
mod utils;
