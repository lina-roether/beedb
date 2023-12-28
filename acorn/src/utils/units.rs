#![allow(non_upper_case_globals)]

use std::usize;

pub const B: usize = 1;
pub const KiB: usize = 1024 * B;
pub const MiB: usize = 1024 * KiB;
pub const GiB: usize = 1024 * MiB;

const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

pub fn display_size(size: usize) -> String {
	let width = size.ilog2();
	let unit = width / 10;
	let unit_name = UNITS[unit as usize];
	let decimal_pos = unit * 10;
	let converted = (size as f64) / ((1 << decimal_pos) as f64);
	format!("{converted} {unit_name}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_display_size() {
		assert_eq!(display_size(20 * GiB), String::from("20 GiB"));
		assert_eq!(
			display_size(30 * MiB + 20 * KiB),
			String::from("30.01953125 MiB")
		);
	}
}
