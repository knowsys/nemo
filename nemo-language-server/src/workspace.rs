//! The documents a session has open.

use std::{collections::HashMap, path::PathBuf};

use crate::document::Document;

/// The open documents, keyed by the URIs that the editor uses for them.
///
/// The server keeps no other state: a document is parsed when it changes,
/// and every request reads it from here.
#[derive(Debug, Default)]
pub(crate) struct Workspace {
    /// The documents, by URI.
    documents: HashMap<String, Document>,
}

impl Workspace {
    /// Creates an empty workspace.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Stores the text of an opened or edited document, parses and checks it,
    /// and returns the document.
    ///
    /// A `@use` in the document reads another open document's text, if there is one,
    /// rather than the file on disk.
    /// If the text does not parse, the document keeps the types of its previous text,
    /// as [inherit_types](Document::inherit_types) describes.
    pub(crate) fn set(&mut self, uri: impl Into<String>, text: String) -> &Document {
        let uri = uri.into();
        let document = {
            let open: Vec<(PathBuf, &str)> = self
                .documents
                .iter()
                .filter(|(other, _)| **other != uri)
                .filter_map(|(other, document)| Some((file_path(other)?, document.text())))
                .collect();
            let mut document = Document::new(text, file_path(&uri), &open);
            if let Some(previous) = self.documents.get(&uri) {
                document.inherit_types(previous);
            }
            document
        };
        self.documents.insert(uri.clone(), document);
        self.documents
            .get(&uri)
            .expect("the document was just inserted")
    }

    /// Removes a closed document.
    pub(crate) fn remove(&mut self, uri: &str) {
        self.documents.remove(uri);
    }

    /// Returns the document a URI addresses, if it is open.
    pub(crate) fn get(&self, uri: &str) -> Option<&Document> {
        self.documents.get(uri)
    }
}

/// Returns the file system path of a `file://` URI, with its escapes decoded.
///
/// Returns `None` for another scheme, for a file on another machine, or for a malformed
/// escape.
/// A document without a path has no directory relative to which its modules are found.
fn file_path(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file://")?;
    let slash = rest.find('/')?;
    let (authority, path) = rest.split_at(slash);
    if !(authority.is_empty() || authority.eq_ignore_ascii_case("localhost")) {
        return None;
    }
    let decoded = percent_decode(path)?;
    // A Windows drive path arrives as `/c:/…`, where the leading slash belongs to the URI,
    // not to the path.
    let bytes = decoded.as_bytes();
    if bytes.len() >= 3 && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
        return Some(PathBuf::from(&decoded[1..]));
    }
    Some(PathBuf::from(decoded))
}

/// Decodes the `%xx` escapes of a text as UTF-8 bytes.
///
/// Returns `None` if an escape is malformed or the result is not UTF-8,
/// so that a path is never guessed.
fn percent_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' => {
                let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
                decoded.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Setting a document's text again replaces its index, and removing the document closes
    /// it.
    #[test]
    fn a_document_is_reparsed_when_it_changes() {
        let mut workspace = Workspace::new();
        workspace.set("file:///a.rls", "p(1) .\n".to_string());
        assert_eq!(
            workspace
                .get("file:///a.rls")
                .expect("it is open")
                .symbols()
                .occurrences("p")
                .count(),
            1
        );

        workspace.set("file:///a.rls", "q(1) .\n".to_string());
        let document = workspace.get("file:///a.rls").expect("it is still open");
        assert_eq!(document.symbols().occurrences("p").count(), 0);
        assert_eq!(document.symbols().occurrences("q").count(), 1);

        workspace.remove("file:///a.rls");
        assert!(workspace.get("file:///a.rls").is_none());
    }

    /// A `@use` resolves to an open document whose file does not exist on disk.
    #[test]
    fn a_use_resolves_to_an_open_document() {
        // Neither file exists on disk. The module is the text that the editor holds,
        // and the document that uses it is checked against that text.
        let mut workspace = Workspace::new();
        workspace.set(
            "file:///nowhere/tc.rls",
            "@input edge .\n@output reach .\nreach(?x, ?y) :- edge(?x, ?y) .\n".to_string(),
        );
        let document = workspace.set(
            "file:///nowhere/main.rls",
            "link(1, 2) .\n@use tc { edge = link } as t .\nout(?x) :- t::reach(?x, ?y) .\n"
                .to_string(),
        );
        assert!(
            document.diagnostics().is_empty(),
            "{:?}",
            document.diagnostics()
        );
    }

    /// A local `file://` URI resolves to its decoded path, and any other URI to no path.
    #[test]
    fn file_uris_resolve_to_paths() {
        assert_eq!(
            file_path("file:///c:/rules/my%20program.rls"),
            Some(PathBuf::from("c:/rules/my program.rls"))
        );
        assert_eq!(
            file_path("file:///home/alex/closure.rls"),
            Some(PathBuf::from("/home/alex/closure.rls"))
        );
        assert_eq!(
            file_path("file://localhost/etc/a.rls"),
            Some(PathBuf::from("/etc/a.rls"))
        );
        assert_eq!(file_path("untitled:Untitled-1"), None, "no scheme, no path");
        assert_eq!(file_path("file://server/share/a.rls"), None, "not local");
        assert_eq!(file_path("file:///bad%zz"), None, "a malformed escape");
    }
}
