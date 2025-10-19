//! This module defines how Nemo programs should be pretty-printed.

/// The target line length
pub const LINE_LENGTH: usize = 88;

/// How much the indent level increases, when not aligned to other characters.
pub const INDENT_INCREMENT: usize = 2;

/// Check whether the given text fits on the remainder of the line at
/// the given indent level, optionally including the given prefix and
/// suffix.
pub fn fits_on_line(
    text: &str,
    indent_level: usize,
    prefix: Option<&str>,
    suffix: Option<&str>,
) -> bool {
    let total_length = indent_level
        + prefix.map(|prefix| prefix.len()).unwrap_or_default()
        + text.len()
        + suffix.map(|suffix| suffix.len()).unwrap_or_default();

    total_length <= LINE_LENGTH
}

/// Wrap the given text to fit the [line length][LINE_LENGTH],
/// indenting each line by the given level and optionally adding the
/// given prefix.
pub fn wrap_lines(text: &str, indent_level: usize, line_prefix: Option<&str>) -> String {
    let prefix_length = indent_level + line_prefix.map(|prefix| prefix.len()).unwrap_or_default();
    let new_line_prefix = format!(
        "\n{}{}",
        " ".repeat(indent_level),
        line_prefix.unwrap_or("")
    );
    let mut result = line_prefix.unwrap_or_default().to_string();
    let mut current_line = prefix_length;

    for word in text.split_ascii_whitespace() {
        let word_length = word.len();
        if word_length + prefix_length <= LINE_LENGTH || current_line == prefix_length {
            current_line += word_length;
            result.push_str(word);
        } else {
            current_line = prefix_length + word_length;
            result.push_str(&new_line_prefix);
            result.push_str(word);
        }
    }

    result
}
