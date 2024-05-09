pub mod display;

#[macro_export]
macro_rules! assert_buf_eq {
    ($left:expr, $right:expr$(,)?) => {
        $crate::assert_buf_eq!(@ $left, $right, "", "")
    };
    ($left:expr, $right:expr, $($arg:tt)*) => {
        $crate::assert_buf_eq!(@ $left, $right, ": ", $($arg:tt)*)
    };
	(@ $left:expr, $right:expr, $maybe_colon:expr, $($arg:tt)*) => {
        if $left != $right {
            ::core::panic!("assertion failed: `(left == right)`{}{}\n\n{}\n", $maybe_colon, format_args!($($arg)*), $crate::display::hexdump::HexdumpDiff::new(&$left, &$right))
        }
    };
}
