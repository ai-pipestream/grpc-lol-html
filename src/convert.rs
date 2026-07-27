// SPDX-License-Identifier: Apache-2.0

//! Conversions from lol-html's content types to the wire model.
//!
//! Every function here is total: lol-html cannot hand us a value that has no
//! representation on the wire. [`text_type`] is an exhaustive match for that
//! reason, so adding a text type upstream breaks the build here rather than
//! silently arriving at clients as `UNSPECIFIED`.

use lol_html::html_content::{Attribute, SourceLocation, TextType};

use crate::proto::v1 as pb;

/// The XML namespace URIs lol-html reports from `Element::namespace_uri()`.
///
/// lol-html keeps its own `Namespace` enum private and exposes only the URI,
/// so this is the seam. Unlike [`text_type`] it cannot be checked by the
/// compiler, which is why `tests/extract.rs` asserts all three round-trip.
const NS_HTML: &str = "http://www.w3.org/1999/xhtml";
const NS_SVG: &str = "http://www.w3.org/2000/svg";
const NS_MATHML: &str = "http://www.w3.org/1998/Math/MathML";

/// Convert a source location to its wire span.
pub fn source_span(loc: &SourceLocation) -> pb::SourceSpan {
    let bytes = loc.bytes();
    pb::SourceSpan {
        start: bytes.start as u64,
        end: bytes.end as u64,
    }
}

/// Map an element's namespace URI to the wire enum.
pub fn namespace(uri: &str) -> pb::Namespace {
    match uri {
        NS_HTML => pb::Namespace::Html,
        NS_SVG => pb::Namespace::Svg,
        NS_MATHML => pb::Namespace::Mathml,
        _ => pb::Namespace::Unspecified,
    }
}

/// Map a lol-html text type to the wire enum.
///
/// Exhaustive on purpose: `TextType` is not `#[non_exhaustive]`, so a new
/// upstream arm is a compile error here, which is where we want to find out.
pub const fn text_type(text_type: TextType) -> pb::TextType {
    match text_type {
        TextType::Data => pb::TextType::Data,
        TextType::RCData => pb::TextType::Rcdata,
        TextType::RawText => pb::TextType::RawText,
        TextType::ScriptData => pb::TextType::ScriptData,
        TextType::PlainText => pb::TextType::PlainText,
        TextType::CDataSection => pb::TextType::CdataSection,
    }
}

/// Convert one attribute, optionally carrying its source spans.
///
/// # Bare attributes, and why the spans are filtered
///
/// lol-html 3.0.0 reports unusable source locations for an attribute written
/// without a value, such as the `defer` in `<script defer>`. It keeps one
/// position for the name/value pair, and for a bare attribute the value half
/// is never set, so what comes back is whatever that slot happened to hold.
/// Reproduced directly against the library, no gRPC involved, on
/// `<html><body>\n<script SRC="/a.js" DEFER async></script>...`:
///
/// ```text
/// chunk 8:   defer: name=Some(33..38)  value=Some(13..13)
/// chunk 64:  defer: name=None          value=None
/// ```
///
/// Byte 13 is the start of `<script`, nowhere near the attribute, and which
/// of the two shapes you get depends on how the caller split the upload.
/// Handing that to a client would break the one promise this service makes,
/// that chunking is invisible.
///
/// A real value always begins after its own name ends, so a value span
/// starting before the name ends cannot be a location. Both spans are dropped
/// for such an attribute, which is deterministic and never wrong. An
/// explicitly empty value, `foo=""`, still points where it should and keeps
/// its spans.
pub fn attribute(attr: &Attribute<'_>, want_spans: bool) -> pb::Attribute {
    let (name_span, value_span) = if want_spans {
        attribute_spans(attr)
    } else {
        (None, None)
    };

    pb::Attribute {
        name: attr.name(),
        name_raw: attr.name_preserve_case(),
        value: attr.value(),
        name_span,
        value_span,
    }
}

/// Resolve an attribute's spans, discarding the pair when it is incoherent.
fn attribute_spans(attr: &Attribute<'_>) -> (Option<pb::SourceSpan>, Option<pb::SourceSpan>) {
    let name = attr.name_source_location().as_ref().map(source_span);
    let value = attr.value_source_location().as_ref().map(source_span);

    if let (Some(name), Some(value)) = (&name, &value)
        && value.start < name.end
    {
        return (None, None);
    }

    (name, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_text_type_has_a_distinct_wire_value() {
        let all = [
            TextType::Data,
            TextType::RCData,
            TextType::RawText,
            TextType::ScriptData,
            TextType::PlainText,
            TextType::CDataSection,
        ];
        let mapped: Vec<_> = all.iter().copied().map(text_type).collect();

        // None collapse onto each other, and none land on the zero value.
        for (i, a) in mapped.iter().enumerate() {
            assert_ne!(*a, pb::TextType::Unspecified, "arm {i} maps to Unspecified");
            for b in &mapped[i + 1..] {
                assert_ne!(a, b, "two text types share a wire value");
            }
        }
    }

    #[test]
    fn namespace_uris_map_and_anything_else_does_not() {
        assert_eq!(namespace(NS_HTML), pb::Namespace::Html);
        assert_eq!(namespace(NS_SVG), pb::Namespace::Svg);
        assert_eq!(namespace(NS_MATHML), pb::Namespace::Mathml);
        assert_eq!(
            namespace("http://example.com/ns"),
            pb::Namespace::Unspecified
        );
    }
}
