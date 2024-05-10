use std::{borrow::Borrow, fmt};

const DELIMITER: &str = "\x1b[1;37m|\x1b[0m";
const DELIMITER_WIDTH: usize = 3;
const BYTE_WIDTH: usize = 2;
const BYTE_SPACE: &str = " ";
const BYTE_SPACE_WIDTH: usize = 1;
const PADDING: &str = "   ";
const PADDING_WIDTH: usize = 3;

fn write_hexdump_diff_line(
	f: &mut fmt::Formatter,
	bytes: &[u8],
	compare: &[u8],
	highlight: &str,
	pad_to: usize,
) -> fmt::Result {
	let line_length = usize::max(bytes.len(), compare.len());
	write!(f, "{PADDING}")?;
	for i in 0..line_length {
		let byte_str = bytes
			.get(i)
			.map(|byte| format!("{byte:02x}"))
			.unwrap_or("  ".to_string());
		let color = if bytes.get(i) != compare.get(i) {
			highlight
		} else {
			"\x1b[90m"
		};
		write!(f, "{color}{byte_str}\x1b[0m")?;
		if i != line_length - 1 {
			write!(f, "{BYTE_SPACE}")?;
		}
	}
	if line_length < pad_to {
		for _ in line_length..pad_to {
			write!(f, "{BYTE_SPACE}  ")?;
		}
	}
	write!(f, "{PADDING}")?;
	Ok(())
}

fn get_terminal_width() -> Option<usize> {
	Some(termsize::get()?.cols.into())
}

fn get_bytes_per_line(width: usize) -> usize {
	(width + BYTE_SPACE_WIDTH - 2 * PADDING_WIDTH) / (BYTE_WIDTH + BYTE_SPACE_WIDTH)
}

fn write_centered(f: &mut fmt::Formatter, message: String, width: usize) -> fmt::Result {
	if message.len() >= width {
		write!(f, "{message}")?;
	} else {
		let padding = (width - message.len()) / 2;
		write!(f, "{}{message}", " ".repeat(padding))?;
	}
	Ok(())
}

fn write_padded_number(f: &mut fmt::Formatter, number: usize, pad_to: usize) -> fmt::Result {
	let width = (number.checked_ilog10().unwrap_or(0) + 1) as usize;
	let num_zeroes = pad_to.saturating_sub(width);
	write!(f, "\x1b[37m{}", " ".repeat(num_zeroes))?;
	write!(f, "{number}\x1b[0m")?;

	Ok(())
}

pub struct HexdumpDiff<'a> {
	pub received: &'a [u8],
	pub expected: &'a [u8],
}

impl<'a> HexdumpDiff<'a> {
	pub fn new(received: &'a impl Borrow<[u8]>, expected: &'a impl Borrow<[u8]>) -> Self {
		Self {
			received: received.borrow(),
			expected: expected.borrow(),
		}
	}
}

impl fmt::Display for HexdumpDiff<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let Some(terminal_width) = f.width().or(get_terminal_width()) else {
			writeln!(f, "\x1b[91mCould not determine terminal size!")?;
			return Ok(());
		};

		let diff_len = usize::max(self.received.len(), self.expected.len());
		if diff_len == 0 {
			write_centered(
				f,
				"\x1b[37mBoth buffers are empty\x1b[0m".to_string(),
				terminal_width,
			)?;
		}

		let index_width = (diff_len.ilog10() + 1) as usize;
		let line_length = (terminal_width - index_width - DELIMITER_WIDTH) / 2;

		let bytes_per_line = get_bytes_per_line(line_length);
		let mut num_lines = diff_len / bytes_per_line;
		if diff_len % bytes_per_line != 0 {
			num_lines += 1;
		}

		let pad_to = if num_lines == 1 { 0 } else { bytes_per_line };

		let mut last_received_line: Option<Box<[u8]>> = None;
		let mut last_expected_line: Option<Box<[u8]>> = None;
		let mut same_line_counter: usize = 0;

		for line in 0..num_lines {
			let offset = line * bytes_per_line;
			let received_end = usize::min(self.received.len(), offset + bytes_per_line);
			let expected_end = usize::min(self.expected.len(), offset + bytes_per_line);
			let received_line = self.received.get(offset..received_end).unwrap_or_default();
			let expected_line = self.expected.get(offset..expected_end).unwrap_or_default();

			if last_received_line == Some(received_line.into())
				&& last_expected_line == Some(expected_line.into())
			{
				same_line_counter += 1;
				continue;
			} else {
				if same_line_counter != 0 {
					write!(f, "\x1b[90m")?;
					write_centered(
						f,
						format!("... {same_line_counter} repetitions ..."),
						terminal_width,
					)?;
					writeln!(f, "\x1b[0m")?;
				}
				same_line_counter = 0;
				last_received_line = Some(received_line.into());
				last_expected_line = Some(expected_line.into());
			}

			write_padded_number(f, offset, index_width)?;
			write_hexdump_diff_line(f, received_line, expected_line, "\x1b[1;91m", pad_to)?;
			write!(f, "{DELIMITER}")?;
			write_hexdump_diff_line(f, expected_line, received_line, "\x1b[1;92m", pad_to)?;
			writeln!(f)?;
		}
		Ok(())
	}
}
