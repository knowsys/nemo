//! Finding the line ends that end a record where fields may be quoted.

use memchr::memchr;

use crate::load::batch::RowEnds;

/// The byte order mark of UTF-8, which the parser of records skips at the start of a resource.
const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

/// Where a scan is within a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    /// At the start of a field, where a quote opens a quoted field.
    FieldStart,
    /// Inside a field that is not quoted, where a quote is a character like any other.
    Unquoted,
    /// Inside a quoted field, where a line end is part of the field.
    Quoted,
    /// After a quote inside a quoted field.
    ///
    /// A second quote makes the two a quote character of the field.
    /// Any other byte continues the field without quotes, so the first quote closed them.
    AfterQuote,
}

/// A scan of delimiter-separated values with quoted fields for the line ends that end a record.
///
/// A line end inside a quoted field is part of the field.
/// Whether a line end is inside one depends on every quote before it,
/// so the scan reads the resource from its start.
/// It follows the rules of the parser of records, the `csv` crate with quoting:
/// - a quote opens a quoted field only as the first byte of a field;
/// - inside a quoted field, two quotes are one quote character, and a single quote closes;
/// - after the closing quote, the field continues without quotes up to its delimiter.
#[derive(Debug)]
pub(crate) struct QuotedRecordEnds {
    /// The delimiter of the fields.
    delimiter: u8,
    /// Where the scan is.
    place: Place,
    /// Whether the scan has read the start of the resource.
    started: bool,
}

impl QuotedRecordEnds {
    /// Creates a scan for fields with the given delimiter, at the start of a resource.
    pub(crate) fn new(delimiter: u8) -> Self {
        Self {
            delimiter,
            place: Place::FieldStart,
            started: false,
        }
    }
}

impl RowEnds for QuotedRecordEnds {
    fn advance(&mut self, bytes: &[u8], from: usize) -> Option<usize> {
        let mut position = 0;
        if !self.started {
            self.started = true;
            if bytes.starts_with(BYTE_ORDER_MARK) {
                position = BYTE_ORDER_MARK.len();
            }
        }

        while position < bytes.len() {
            match self.place {
                Place::Quoted => {
                    let quote = memchr(b'"', &bytes[position..])?;
                    position += quote + 1;
                    self.place = Place::AfterQuote;
                }
                Place::AfterQuote if bytes[position] == b'"' => {
                    position += 1;
                    self.place = Place::Quoted;
                }
                Place::AfterQuote => self.place = Place::Unquoted,
                Place::FieldStart | Place::Unquoted => {
                    // No byte before the next quote is inside a quoted field,
                    // so every line end before it ends a record.
                    let rest = &bytes[position..];
                    let stretch = memchr(b'"', rest).unwrap_or(rest.len());
                    let end = position + stretch;
                    let wanted = from.max(position);
                    if wanted < end
                        && let Some(line_end) = memchr(b'\n', &bytes[wanted..end])
                    {
                        self.place = Place::FieldStart;
                        return Some(wanted + line_end + 1);
                    }

                    if let Some(&last) = rest[..stretch].last() {
                        self.place = if last == self.delimiter || last == b'\n' || last == b'\r' {
                            Place::FieldStart
                        } else {
                            Place::Unquoted
                        };
                    }

                    position = end;
                    if position < bytes.len() {
                        // The quote opens a quoted field only at the start of a field.
                        position += 1;
                        if self.place == Place::FieldStart {
                            self.place = Place::Quoted;
                        }
                    }
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use csv::{ByteRecord, ReaderBuilder};

    /// Returns the records that the parser of records reads from the bytes.
    fn records(bytes: &[u8]) -> Vec<ByteRecord> {
        ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(bytes)
            .byte_records()
            .map(|record| record.expect("bytes in memory are read"))
            .collect()
    }

    /// Returns the first cut of the bytes at index `from` or after it,
    /// with the bytes passed to the scan in pieces of the given size.
    fn cut(bytes: &[u8], from: usize, piece: usize) -> Option<usize> {
        let mut scan = QuotedRecordEnds::new(b',');
        let mut position = 0;
        for bytes in bytes.chunks(piece) {
            let read = scan.advance(bytes, from.saturating_sub(position).min(bytes.len()));
            if let Some(read) = read {
                return Some(position + read);
            }

            position += bytes.len();
        }

        None
    }

    /// A pseudo-random sequence of numbers, the same in every run.
    struct Numbers(u64);

    impl Numbers {
        /// Returns the next number below `bound`.
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;

            usize::try_from(self.0 % bound as u64).expect("the number is below the bound")
        }
    }

    /// A line end inside a quoted field is no cut, and a quote inside a field opens none.
    #[test]
    fn quoted_line_ends_are_no_cuts() {
        // The first line end is inside the quoted field, and the second ends the record.
        let bytes = b"1,\"line one\nline two\"\n2,x\n";
        assert_eq!(cut(bytes, 0, bytes.len()), Some(22));

        // Two quotes inside a quoted field are a quote character, so the field goes on.
        let bytes = b"\"a\"\"\nb\"\nc\n";
        assert_eq!(cut(bytes, 0, bytes.len()), Some(8));

        // A quote that does not start its field is a character of the field.
        let bytes = b"a\"b\nc\"d\n";
        assert_eq!(cut(bytes, 0, bytes.len()), Some(4));

        // After the closing quote, the field continues without quotes.
        let bytes = b"\"a\"b\"\nc\n";
        assert_eq!(cut(bytes, 0, bytes.len()), Some(6));

        // The byte order mark does not keep the first quote from opening a field.
        let bytes = b"\xEF\xBB\xBF\"a\nb\"\nc\n";
        assert_eq!(cut(bytes, 0, bytes.len()), Some(9));
    }

    /// Cutting where the scan says so does not change the records that are read,
    /// whatever the bytes are and however they are passed to the scan,
    /// and the scan finds a cut where a record starts after a line end.
    #[test]
    fn a_cut_keeps_the_records() {
        let alphabet = b"\"\"\",,\n\n\rab ";
        let mut numbers = Numbers(0x9E37_79B9_7F4A_7C15);
        for _ in 0..10_000 {
            let length = 1 + numbers.below(24);
            let bytes: Vec<u8> = (0..length)
                .map(|_| alphabet[numbers.below(alphabet.len())])
                .collect();
            let from = numbers.below(length);
            let piece = 1 + numbers.below(length);
            let text = String::from_utf8_lossy(&bytes).into_owned();

            let whole = records(&bytes);
            let found = cut(&bytes, from, piece);
            if let Some(cut) = found {
                assert!(cut > from && bytes[cut - 1] == b'\n', "{text:?} at {cut}");

                let mut parts = records(&bytes[..cut]);
                parts.extend(records(&bytes[cut..]));
                assert_eq!(parts, whole, "{text:?} cut at {cut} from {from}");
            }

            // A record that starts directly after a line end starts at a cut.
            let mut reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(&bytes[..]);
            let mut record = ByteRecord::new();
            while reader
                .read_byte_record(&mut record)
                .expect("bytes are read")
            {
                let start = record.position().expect("a read record").byte();
                let start = usize::try_from(start).expect("the bytes are in memory");
                if start > from && bytes[start - 1] == b'\n' {
                    assert!(
                        found.is_some_and(|cut| cut <= start),
                        "{text:?} has a record at {start}, and the cut from {from} is {found:?}"
                    );
                }
            }
        }
    }
}
