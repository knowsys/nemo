//! The HTML report: one self-contained page over a document.

/// The page template, whose placeholders [render] substitutes.
const TEMPLATE: &str = include_str!("page.html");

/// Renders the report page over a document's JSON (see [ReportDocument::page](super::ReportDocument::page)).
///
/// The JSON is embedded in a `<script>` element,
/// and the HTML parser ends that element at the first `</script>`, even inside a string.
/// So `<`, `>` and `&` are written as `\u` escapes.
/// So are U+2028 and U+2029, which JSON allows in strings but JavaScript source does not.
pub(super) fn render(document: &str, live: bool) -> String {
    let embedded = document
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    // The live flag first: the document is arbitrary JSON and could
    // contain any placeholder, so it must be substituted last.
    TEMPLATE
        .replacen("__LIVE__", if live { "true" } else { "false" }, 1)
        .replacen("__DOCUMENT__", &embedded, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::telemetry::ReportDocument;

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

    #[test]
    fn a_document_containing_a_placeholder_is_embedded_unchanged() {
        let page = render("{\"title\":\"__LIVE__\"}", true);
        assert!(page.contains("const LIVE = true;"));
        assert!(page.contains("const DOCUMENT = {\"title\":\"__LIVE__\"};"));
    }

    #[test]
    fn documents_cannot_end_the_page_script_early() {
        // A title (or a rule, a resource, a program text) containing the
        // script's end tag must not end the script.
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
