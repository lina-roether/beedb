#![feature(pointer_is_aligned)]
#![feature(ptr_metadata)]

mod buf;
mod slice;
mod transmute;
mod view;

pub use buf::*;
pub use slice::*;
pub use transmute::*;
pub use view::*;

#[cfg(feature = "derive")]
#[allow(unused)]
pub use byte_view_macros::ByteView;
