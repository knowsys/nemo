//! The HTML report: one self-contained page that shows a document.

/// The page template, whose placeholders [render] substitutes.
const TEMPLATE: &str = include_str!("page.html");

/// Renders the report page that shows a document's JSON.
///
/// `live` is the flag of [ReportDocument::page](super::ReportDocument::page).
/// The JSON is embedded in a `<script>` element,
/// and the HTML parser ends that element at the first `</script>`, even inside a string.
/// So `<`, `>` and `&` are written as `\u` escapes.
/// So are U+2028 and U+2029, which JSON allows in strings but JavaScript before ES2019 does
/// not.
pub(super) fn render(document: &str, live: bool) -> String {
    let embedded = document
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    // The document is substituted last, since it is arbitrary JSON that could contain a
    // placeholder.
    TEMPLATE
        .replacen("__LIVE__", if live { "true" } else { "false" }, 1)
        .replacen("__DOCUMENT__", &embedded, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::telemetry::ReportDocument;

    /// A page embeds the document and sets the live flag.
    #[test]
    fn pages_embed_the_document_and_the_mode() {
        let page = render("{\"title\":\"t\",\"run\":null,\"telemetry\":null}", false);
        assert!(page.contains("const LIVE = false;"), "the mode is set");
        assert!(
            page.contains("const DOCUMENT = {\"title\":\"t\",\"run\":null,\"telemetry\":null};"),
            "the document is embedded"
        );
        assert!(!page.contains("__DOCUMENT__"));
        assert!(!page.contains("__LIVE__"));
    }

    /// A document that contains a placeholder is embedded unchanged.
    #[test]
    fn a_document_containing_a_placeholder_is_embedded_unchanged() {
        let page = render("{\"title\":\"__LIVE__\"}", true);
        assert!(page.contains("const LIVE = true;"));
        assert!(page.contains("const DOCUMENT = {\"title\":\"__LIVE__\"};"));
    }

    /// A document cannot end the page's script early or add markup,
    /// and its line separators are escaped.
    #[test]
    fn documents_cannot_end_the_page_script_early() {
        // Any text of the document, such as a title, a rule, a resource or a program text,
        // may contain the script's end tag.
        let page = ReportDocument {
            title: "</script><img src=x onerror=alert(1)>\u{2028}",
            run: None,
            telemetry: None,
        }
        .page(false);
        assert_eq!(page.matches("</script>").count(), 1, "{page}");
        assert!(!page.contains("<img"), "{page}");
        assert!(!page.contains('\u{2028}'), "{page}");
    }
}
