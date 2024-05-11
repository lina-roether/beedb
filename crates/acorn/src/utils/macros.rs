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
