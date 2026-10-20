//! Nondeterministic values: randomness and UUIDs.
//!
//! Every evaluation draws fresh from the thread's generator of `rand`,
//! which the platform's entropy seeds, in the browser JavaScript's crypto API,
//! so a program with one of these instructions
//! is [nondeterministic](crate::function::StackProgram::is_nondeterministic).
//! The run's instant is no draw: it comes from the
//! [context](crate::function::EvaluationContext).

use crate::{function::StackValue, value::DataValue};

/// Returns a random double in `[0, 1)`,
/// SPARQL's [`RAND`](https://www.w3.org/TR/sparql12-query/#func-rand).
pub(crate) fn random() -> StackValue<'static> {
    DataValue::double(rand::random::<f64>()).into()
}

/// Returns a fresh UUID of version 4, 122 random bits, as text.
fn uuid_text() -> String {
    let mut bits = rand::random::<u128>();

    // The version nibble and the two variant bits of RFC 9562.
    bits = (bits & !(0xf << 76)) | (0x4 << 76);
    bits = (bits & !(0x3 << 62)) | (0x2 << 62);
    let hex = format!("{bits:032x}");

    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// Returns a fresh UUID as a `urn:uuid:` IRI,
/// SPARQL's [`UUID`](https://www.w3.org/TR/sparql12-query/#func-uuid).
pub(crate) fn uuid_iri() -> StackValue<'static> {
    DataValue::iri(format!("urn:uuid:{}", uuid_text())).into()
}

/// Returns a fresh UUID as a plain string,
/// SPARQL's [`STRUUID`](https://www.w3.org/TR/sparql12-query/#func-struuid).
pub(crate) fn uuid_string() -> StackValue<'static> {
    DataValue::string(uuid_text()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuids_have_the_version_4_layout() {
        for _ in 0..8 {
            let uuid = uuid_text();
            let groups: Vec<&str> = uuid.split('-').collect();
            assert_eq!(
                groups.iter().map(|group| group.len()).collect::<Vec<_>>(),
                [8, 4, 4, 4, 12]
            );
            assert!(
                uuid.bytes()
                    .all(|byte| byte == b'-' || byte.is_ascii_hexdigit())
            );
            assert!(groups[2].starts_with('4'));
            assert!(matches!(groups[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b'));
        }
        assert_ne!(uuid_text(), uuid_text());
    }
}
