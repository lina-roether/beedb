use std::{
	mem::size_of,
	num::{
		NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroU128, NonZeroU16,
		NonZeroU32, NonZeroU64, NonZeroU8,
	},
};

use crate::{transmute, transmute_mut, transmute_unsized, transmute_unsized_mut};

/// This trait indicates that it is safe to reinterpret
/// a byte array as this type.
///
/// # Safety
///
/// This trait may only be implemented if **ANY** possible combination of bits
/// is a legal value for this type and doesn't violate any invariants.
pub unsafe trait ByteView {
	const ALIGN: usize;
	const MIN_SIZE: usize;

	fn from_bytes(bytes: &[u8]) -> &Self {
		if bytes.len() < Self::MIN_SIZE {
			panic!(
				"The minimum size for this from_bytes call is {}; got size {}",
				Self::MIN_SIZE,
				bytes.len()
			)
		}
		if !bytes.as_ptr().is_aligned_to(Self::ALIGN) {
			panic!("Cannot use from_bytes on unaligned memory");
		}
		unsafe { Self::from_bytes_unchecked(bytes) }
	}

	fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
		if bytes.len() < Self::MIN_SIZE {
			panic!(
				"The minimum size for this from_bytes call is {}; got size {}",
				Self::MIN_SIZE,
				bytes.len()
			)
		}
		if !bytes.as_ptr().is_aligned_to(Self::ALIGN) {
			panic!("Cannot use from_bytes on unaligned memory");
		}
		unsafe { Self::from_bytes_mut_unchecked(bytes) }
	}

	/// # Safety
	///
	/// This function is only safe to call if the provided slice of bytes
	/// at least the minimum size for this view, and is properly aligned.
	unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self;

	/// # Safety
	///
	/// This function is only safe to call if the provided slice of bytes
	/// at least the minimum size for this view, and is properly aligned.
	unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self;
}

#[macro_export]
macro_rules! assert_sized {
	($ty:ty) => {
		const _: fn() = || {
			fn assert_sized<T: Sized>() {}
			assert_sized::<$ty>();
		};
	};
}

#[macro_export]
macro_rules! assert_byte_view {
	($ty:ty) => {
		const _: fn() = || {
			fn assert_byte_view<T: ?Sized + $crate::ByteView>() {}
			assert_byte_view::<$ty>();
		};
	};
}

#[macro_export]
macro_rules! unsafe_impl_byte_view_sized {
    ($($ty:ty),*) => {
        $(
            $crate::assert_sized!($ty);

            unsafe impl ByteView for $ty {
                const ALIGN: usize = std::mem::align_of::<Self>();
                const MIN_SIZE: usize = std::mem::size_of::<Self>();

                unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
                    $crate::transmute(bytes)
                }

                unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
                    $crate::transmute_mut(bytes)
                }
            }
         )*
    };
}

// Safety: None of the integer types have internal invariants
unsafe_impl_byte_view_sized!(u8, i8, u16, i16, u32, i32, u64, i64, u128, i128);

// Safety: While the nonzero variants of integers have the internal invariant
// that they disallow zero, for the corresponding option type `None` is
// represented as zero, so this is safe.
unsafe_impl_byte_view_sized!(
	Option<NonZeroU8>,
	Option<NonZeroI8>,
	Option<NonZeroU16>,
	Option<NonZeroI16>,
	Option<NonZeroU32>,
	Option<NonZeroI32>,
	Option<NonZeroU64>,
	Option<NonZeroI64>,
	Option<NonZeroU128>,
	Option<NonZeroI128>
);

// Safety: The floating point standard has no undefined bit patterns,
// so this should be safe. There may be some platform-dependent problems,
// but it is what it is...
unsafe_impl_byte_view_sized!(f32, f64);

unsafe impl<T: ByteView, const N: usize> ByteView for [T; N] {
	const ALIGN: usize = T::ALIGN;
	const MIN_SIZE: usize = T::MIN_SIZE * N;

	unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
		transmute(bytes)
	}

	unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
		transmute_mut(bytes)
	}
}

unsafe impl<T: ByteView> ByteView for [T] {
	const ALIGN: usize = T::ALIGN;
	const MIN_SIZE: usize = 0;

	unsafe fn from_bytes_unchecked(bytes: &[u8]) -> &Self {
		transmute_unsized(bytes, bytes.len() / size_of::<T>())
	}

	unsafe fn from_bytes_mut_unchecked(bytes: &mut [u8]) -> &mut Self {
		transmute_unsized_mut(bytes, bytes.len() / size_of::<T>())
	}
}
