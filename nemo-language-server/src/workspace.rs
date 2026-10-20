//! The documents a session has open.

use std::{collections::HashMap, path::PathBuf};

use crate::document::Document;

/// The open documents, by the URI the editor addresses them with.
///
/// The server keeps no other state: a document is parsed when it
/// changes and read from here for everything else.
#[derive(Debug, Default)]
pub(crate) struct Workspace {
    /// The documents.
    documents: HashMap<String, Document>,
}

impl Workspace {
    /// Creates an empty workspace.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Takes in a document's text, parsing and checking it,
    /// for an open as much as for an edit, which differ only in what the editor sent.
    ///
    /// The other open documents are what its `@use`s resolve to before the disk is read.
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

    /// Drops a closed document.
    pub(crate) fn remove(&mut self, uri: &str) {
        self.documents.remove(uri);
    }

    /// Returns the document a URI addresses, if it is open.
    pub(crate) fn get(&self, uri: &str) -> Option<&Document> {
        self.documents.get(uri)
    }
}

/// Returns the filesystem path of a `file://` URI, escapes decoded,
/// or `None` for any other scheme, or for a file on another machine,
/// for which the document has no directory to resolve its modules against.
fn file_path(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file://")?;
    let slash = rest.find('/')?;
    let (authority, path) = rest.split_at(slash);
    if !(authority.is_empty() || authority.eq_ignore_ascii_case("localhost")) {
        return None;
    }
    let decoded = percent_decode(path)?;
    // A Windows drive path arrives as `/c:/…`; the leading slash is the
    // URI's, not the path's.
    let bytes = decoded.as_bytes();
    if bytes.len() >= 3 && bytes[1].is_ascii_alphabetic() && bytes[2] == b':' {
        return Some(PathBuf::from(&decoded[1..]));
    }
    Some(PathBuf::from(decoded))
}

/// Decodes `%xx` escapes as UTF-8 bytes,
/// or returns `None` if an escape is malformed, so that a path is never guessed.
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

    #[test]
    fn a_use_resolves_to_an_open_document() {
        // Neither file exists on disk: the module is the buffer the
        // editor holds, and the using document checks against that.
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
