pub(crate) const B: usize = 1;
pub(crate) const KIB: usize = 1024 * B;
pub(crate) const MIB: usize = 1024 * KIB;
pub(crate) const GIB: usize = 1024 * MIB;

#[cfg(test)]
pub(crate) mod test_helpers {
	macro_rules! map {
		($($key:expr => $value:expr),* $(,)?) => {
            std::collections::HashMap::from_iter([
                $(($key, $value)),*
            ].into_iter())
        };
	}
	pub(crate) use map;

	macro_rules! non_zero {
		($num:expr) => {
			std::num::NonZero::<_>::new($num).unwrap()
		};
	}
	pub(crate) use non_zero;
}
