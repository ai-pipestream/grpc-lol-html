// SPDX-License-Identifier: Apache-2.0

//! Validation and compilation of [`ExtractOptions`] into something the driver
//! can hand straight to lol-html.
//!
//! Everything that can fail does so here, before a response stream is opened.
//! A client therefore learns about a bad selector or an unusable encoding as a
//! gRPC status on the call itself, never as an in-band error halfway through
//! a document it has already spent bandwidth uploading.
//!
//! [`ExtractOptions`]: crate::proto::v1::ExtractOptions

use lol_html::{AsciiCompatibleEncoding, MemorySettings, Selector};
use tonic::Status;

use crate::errors::selector_code;
use crate::proto::v1 as pb;

/// Default id reported for events produced by a [`DocumentRule`].
///
/// [`DocumentRule`]: crate::proto::v1::DocumentRule
pub const DEFAULT_DOCUMENT_RULE_ID: &str = "document";

/// Text types reported when a request does not name any.
///
/// Prose and titles: the two that are entity-decoded, and the two a caller
/// indexing a page actually wants. Everything else, notably the contents of
/// `<script>` and `<style>`, has to be asked for by name.
pub const DEFAULT_TEXT_TYPES: [pb::TextType; 2] = [pb::TextType::Data, pb::TextType::Rcdata];

/// Most rules one call may carry.
///
/// Every rule is matched against every element and every match is its own
/// event, so the rule count multiplies both the parse's CPU and its output.
/// Unbounded, one options frame of `*` rules turns a modest document into
/// billions of events.
pub const MAX_RULES: usize = 256;

/// Longest rule or document-rule id accepted, in bytes.
///
/// The id is copied onto every event its rule produces, so a long one is
/// paid for once per match rather than once per call.
pub const MAX_RULE_ID_BYTES: usize = 256;

/// Longest selector accepted, in bytes. Real selectors are a few dozen.
pub const MAX_SELECTOR_BYTES: usize = 4096;

/// What lol-html preallocates for its parsing buffer when told nothing.
const LOL_HTML_PREALLOCATED_BYTES: usize = 1024;

/// One rule with its selector already compiled.
pub struct CompiledRule {
    /// Caller-chosen id, echoed on every event this rule produces.
    pub id: String,
    /// The compiled selector.
    pub selector: Selector,
    /// Whether to report the element's tag name.
    pub tag_name: bool,
    /// Whether to report the element's attributes.
    pub attributes: bool,
    /// Whether to report text inside the element.
    pub text: bool,
    /// Whether to report comments inside the element.
    pub comments: bool,
    /// Whether to report byte spans on everything this rule emits.
    pub spans: bool,
    /// Whether to report the element's end tag.
    pub end_tag: bool,
}

/// A fully validated request, ready to drive a rewriter.
pub struct CompiledOptions {
    /// Selector-scoped rules, in submission order.
    pub rules: Vec<CompiledRule>,
    /// Document-scope rule, if the caller asked for one.
    pub document: Option<DocumentScope>,
    /// Resolved input encoding.
    pub encoding: AsciiCompatibleEncoding,
    /// Whether a `<meta charset>` may override [`Self::encoding`] mid-parse.
    pub adjust_charset_on_meta_tag: bool,
    /// Whether to refuse to guess on ambiguous markup.
    pub strict: bool,
    /// Whether to treat ESI tags as elements.
    pub enable_esi_tags: bool,
    /// Hard cap on parser-buffered state, and separately on text held for
    /// reassembly. Already clamped to the server's ceiling.
    pub max_memory_bytes: usize,
    /// Bytes to preallocate for the parsing buffer: the caller's size, or
    /// lol-html's default of 1 KiB, never more than [`Self::max_memory_bytes`].
    pub preallocated_buffer_bytes: usize,
    /// Whether exceeding the memory cap ends the run gracefully rather than
    /// with a terminal error.
    pub graceful_bail_out: bool,
    /// Whether to emit raw text fragments instead of reassembled text nodes.
    pub raw_text_chunks: bool,
    /// Text types to report.
    pub text_types: Vec<pb::TextType>,
}

/// Document-scope captures.
pub struct DocumentScope {
    /// Id echoed on events this scope produces.
    pub id: String,
    /// Whether to report the doctype.
    pub doctype: bool,
    /// Whether to report document-level comments.
    pub comments: bool,
    /// Whether to report document-level text.
    pub text: bool,
    /// Whether to report byte spans. Document scope has no `Capture` list of
    /// its own, so it follows whether *any* rule asked for spans, which keeps
    /// a caller from having to ask twice.
    pub spans: bool,
}

impl CompiledOptions {
    /// The canonical name of the resolved input encoding, for the
    /// `ExtractStarted` header.
    pub fn encoding_name(&self) -> &'static str {
        let encoding: &'static encoding_rs::Encoding = self.encoding.into();
        encoding.name()
    }

    /// Build lol-html's memory settings.
    ///
    /// Rebuilt rather than stored because `MemorySettings` is `#[repr(C)]`
    /// for the C API and derives neither `Clone` nor `Copy`.
    ///
    /// The preallocation is always set explicitly, because lol-html's own
    /// default of 1 KiB is itself over a limit below 1 KiB. A preallocation
    /// over the limit is skipped without a word in a release build and trips
    /// a `debug_assert!` that kills the call in a debug one.
    pub fn memory_settings(&self) -> MemorySettings {
        let mut settings = MemorySettings::new()
            .with_max_allowed_memory_usage(self.max_memory_bytes)
            .with_preallocated_parsing_buffer_size(self.preallocated_buffer_bytes);
        if self.graceful_bail_out {
            settings = settings.with_graceful_bail_out_on_memory_limit_exceeded(true);
        }
        settings
    }
}

/// Compile and validate a request's options.
///
/// `memory_ceiling` is the server's cap on a call's memory limit: a request
/// asking for more gets the ceiling, and one asking for nothing gets the
/// default or the ceiling, whichever is lower.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` for an empty rule set, a rule set over the
/// shape limits (see [`check_rule_set`]), a rule with no captures, a selector
/// that does not compile, or an encoding lol-html cannot tokenize.
pub fn compile(
    options: &pb::ExtractOptions,
    memory_ceiling: usize,
) -> Result<CompiledOptions, Status> {
    check_rule_set(&options.rules)?;
    if let Some(doc) = &options.document_rule
        && doc.id.len() > MAX_RULE_ID_BYTES
    {
        return Err(Status::invalid_argument(format!(
            "the document_rule id is {} bytes; ids may be at most {MAX_RULE_ID_BYTES} bytes",
            doc.id.len()
        )));
    }

    let document = options.document_rule.as_ref().and_then(|doc| {
        // A document rule that asks for nothing is the same as none at all.
        (doc.doctype || doc.comments || doc.text).then(|| DocumentScope {
            id: if doc.id.is_empty() {
                DEFAULT_DOCUMENT_RULE_ID.to_owned()
            } else {
                doc.id.clone()
            },
            doctype: doc.doctype,
            comments: doc.comments,
            text: doc.text,
            spans: false,
        })
    });

    if options.rules.is_empty() && document.is_none() {
        return Err(Status::invalid_argument(
            "no rules: supply at least one rule, or a document_rule that asks for something",
        ));
    }

    let mut rules = Vec::with_capacity(options.rules.len());
    let mut any_spans = false;

    for rule in &options.rules {
        let compiled = compile_rule(rule)?;
        any_spans |= compiled.spans;
        rules.push(compiled);
    }

    let document = document.map(|mut doc| {
        doc.spans = any_spans;
        doc
    });

    let max_memory_bytes = max_memory_bytes(options.limits.as_ref(), memory_ceiling);

    Ok(CompiledOptions {
        rules,
        document,
        encoding: resolve_encoding(&options.encoding)?,
        adjust_charset_on_meta_tag: options.adjust_charset_on_meta_tag,
        // lol-html's flag is positive; ours is the opt-in to the unsafe side,
        // so that the proto3 zero value is the safe one.
        strict: !options.allow_ambiguous_markup,
        enable_esi_tags: options.enable_esi_tags,
        max_memory_bytes,
        preallocated_buffer_bytes: preallocated_buffer_bytes(
            options.limits.as_ref(),
            max_memory_bytes,
        ),
        graceful_bail_out: options
            .limits
            .as_ref()
            .is_some_and(|limits| limits.graceful_bail_out),
        raw_text_chunks: options.raw_text_chunks,
        text_types: text_types(&options.text_types),
    })
}

/// Check a rule set against the limits on its shape: at most [`MAX_RULES`]
/// rules, ids of at most [`MAX_RULE_ID_BYTES`] and selectors of at most
/// [`MAX_SELECTOR_BYTES`].
///
/// Shared by `Extract` and `ValidateSelectors`, so a rule set the second
/// accepts is never refused by the first for its size.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` naming the first limit exceeded.
pub fn check_rule_set(rules: &[pb::ExtractRule]) -> Result<(), Status> {
    if rules.len() > MAX_RULES {
        return Err(Status::invalid_argument(format!(
            "{} rules; a call may carry at most {MAX_RULES}. Every rule is matched against \
             every element, so split a larger set across calls.",
            rules.len()
        )));
    }

    for (index, rule) in rules.iter().enumerate() {
        // The id is not echoed: it is the thing that is too long.
        if rule.id.len() > MAX_RULE_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "rule {index} has an id of {} bytes; ids may be at most {MAX_RULE_ID_BYTES} bytes",
                rule.id.len()
            )));
        }
        if rule.selector.len() > MAX_SELECTOR_BYTES {
            return Err(Status::invalid_argument(format!(
                "rule `{}` has a selector of {} bytes; selectors may be at most \
                 {MAX_SELECTOR_BYTES} bytes",
                rule.id,
                rule.selector.len()
            )));
        }
    }

    Ok(())
}

/// Compile one rule, mapping its captures onto flags.
fn compile_rule(rule: &pb::ExtractRule) -> Result<CompiledRule, Status> {
    if rule.captures.is_empty() {
        return Err(Status::invalid_argument(format!(
            "rule `{}` requests no captures, so it could only ever report that \
             something matched; ask for at least one",
            rule.id
        )));
    }

    let selector = rule.selector.parse::<Selector>().map_err(|err| {
        let (code, detail) = selector_code(&err);
        let detail = if detail.is_empty() {
            String::new()
        } else {
            format!(" (`{detail}`)")
        };
        Status::invalid_argument(format!(
            "rule `{}` selector `{}` did not compile: {err}{detail} [{}]. \
             Call ValidateSelectors for the typed diagnostics.",
            rule.id,
            rule.selector,
            code.as_str_name(),
        ))
    })?;

    let has = |capture: pb::Capture| rule.captures.contains(&(capture as i32));

    Ok(CompiledRule {
        id: rule.id.clone(),
        selector,
        tag_name: has(pb::Capture::TagName),
        attributes: has(pb::Capture::Attributes),
        text: has(pb::Capture::Text),
        comments: has(pb::Capture::Comments),
        spans: has(pb::Capture::SourceLocation),
        end_tag: has(pb::Capture::EndTag),
    })
}

/// Resolve a charset label to an encoding lol-html can tokenize.
///
/// # Errors
///
/// Returns `INVALID_ARGUMENT` for an unknown label, and for a known but
/// ASCII-incompatible one.
fn resolve_encoding(label: &str) -> Result<AsciiCompatibleEncoding, Status> {
    if label.is_empty() {
        return Ok(AsciiCompatibleEncoding::utf_8());
    }

    let encoding = encoding_rs::Encoding::for_label(label.as_bytes()).ok_or_else(|| {
        Status::invalid_argument(format!(
            "unknown encoding label `{label}`; use a WHATWG label such as `utf-8` or `windows-1251`"
        ))
    })?;

    AsciiCompatibleEncoding::new(encoding).ok_or_else(|| {
        Status::invalid_argument(format!(
            "encoding `{}` is not ASCII-compatible and cannot be tokenized by lol-html: \
             its tokenizer scans for ASCII markup bytes, which do not mean what they say \
             in this encoding. Transcode the document to UTF-8 first.",
            encoding.name(),
        ))
    })
}

/// Resolve the memory cap, applying our own default and the server's ceiling.
///
/// lol-html defaults `max_allowed_memory_usage` to `usize::MAX`. A server
/// taking documents off the open web must not run that way, so an unset or
/// zero limit becomes [`DEFAULT_MAX_MEMORY_BYTES`] rather than infinity. And
/// the caller does not get the last word: a limit above `ceiling` is cut to
/// it, or any caller could switch the only parser memory guard off by asking
/// for `u64::MAX`.
fn max_memory_bytes(limits: Option<&pb::MemoryLimits>, ceiling: usize) -> usize {
    let max = limits
        .map(|limits| limits.max_bytes)
        .filter(|max| *max > 0)
        .unwrap_or(DEFAULT_MAX_MEMORY_BYTES);
    usize::try_from(max).unwrap_or(usize::MAX).min(ceiling)
}

/// Resolve the parsing-buffer preallocation, never above the memory cap.
///
/// lol-html charges the preallocation against the same limit, so one larger
/// than the limit cannot be honoured: release builds skip it silently and
/// debug builds panic. Clamping keeps the request meaningful in both.
fn preallocated_buffer_bytes(limits: Option<&pb::MemoryLimits>, max_memory_bytes: usize) -> usize {
    limits
        .map(|limits| limits.preallocated_buffer_bytes)
        .filter(|size| *size > 0)
        .map_or(LOL_HTML_PREALLOCATED_BYTES, |size| {
            usize::try_from(size).unwrap_or(usize::MAX)
        })
        .min(max_memory_bytes)
}

/// Default cap on parser-buffered state, when a request names none, and the
/// default for the server's ceiling on what a request may name.
///
/// Generous enough that no conforming document reaches it, small enough that a
/// crafted one cannot exhaust the host.
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

/// Resolve the requested text types, applying the default when none are named
/// and dropping anything unrecognized.
fn text_types(requested: &[i32]) -> Vec<pb::TextType> {
    if requested.is_empty() {
        return DEFAULT_TEXT_TYPES.to_vec();
    }

    let mut types: Vec<pb::TextType> = requested
        .iter()
        .filter_map(|value| pb::TextType::try_from(*value).ok())
        .filter(|text_type| *text_type != pb::TextType::Unspecified)
        .collect();
    types.sort_unstable_by_key(|text_type| *text_type as i32);
    types.dedup();

    if types.is_empty() {
        DEFAULT_TEXT_TYPES.to_vec()
    } else {
        types
    }
}

/// Compile every selector and report the ones that failed.
///
/// Unlike [`compile`] this reports *all* failures rather than stopping at the
/// first, which is the point of having it: one round trip tells a caller
/// everything wrong with their rule set.
pub fn diagnose(rules: &[pb::ExtractRule]) -> Vec<pb::SelectorDiagnostic> {
    rules
        .iter()
        .filter_map(|rule| {
            rule.selector
                .parse::<Selector>()
                .err()
                .map(|err| crate::errors::selector_diagnostic(&rule.id, &rule.selector, &err))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(id: &str, selector: &str) -> pb::ExtractRule {
        pb::ExtractRule {
            id: id.to_owned(),
            selector: selector.to_owned(),
            captures: vec![pb::Capture::TagName as i32],
        }
    }

    /// The server ceiling the tests compile under, unless they are about it.
    const CEILING: usize = DEFAULT_MAX_MEMORY_BYTES as usize;

    /// `CompiledOptions` holds a `Selector`, which is not `Debug`, so
    /// `unwrap_err` is unavailable here.
    fn rejection(options: &pb::ExtractOptions) -> Status {
        match compile(options, CEILING) {
            Ok(_) => panic!("expected the request to be rejected"),
            Err(err) => err,
        }
    }

    fn compiled(options: &pb::ExtractOptions, ceiling: usize) -> CompiledOptions {
        compile(options, ceiling).unwrap_or_else(|err| panic!("rejected: {err}"))
    }

    fn with_limits(limits: pb::MemoryLimits) -> pb::ExtractOptions {
        pb::ExtractOptions {
            rules: vec![rule("a", "div")],
            limits: Some(limits),
            ..Default::default()
        }
    }

    #[test]
    fn an_empty_request_is_refused() {
        let err = rejection(&pb::ExtractOptions::default());
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn a_rule_with_no_captures_is_refused() {
        let options = pb::ExtractOptions {
            rules: vec![pb::ExtractRule {
                captures: vec![],
                ..rule("a", "div")
            }],
            ..Default::default()
        };
        let err = rejection(&options);
        assert!(err.message().contains("no captures"), "{}", err.message());
    }

    /// The supported subset is exactly what a forward-only matcher can decide.
    ///
    /// `:first-child` against `:last-child` is the whole rule: an element is
    /// known to be first the moment it is seen, and cannot be known to be
    /// last until its parent closes. Pinned here because it is the boundary
    /// callers most often guess wrong, in both directions.
    #[test]
    fn backward_looking_selectors_compile_and_forward_looking_ones_do_not() {
        let supported = [
            "*",
            "div p",
            "div > p",
            "a.cls#id",
            "a[href^='http' i]",
            "li:nth-child(2)",
            "li:first-child",
            "li:nth-of-type(2)",
            "li:first-of-type",
            "p:not(.skip)",
        ];
        for selector in supported {
            assert!(
                diagnose(&[rule("r", selector)]).is_empty(),
                "expected `{selector}` to compile"
            );
        }

        let needs_the_future = [
            "li:last-child",
            "li:only-child",
            "li:nth-last-child(2)",
            "li:last-of-type",
            "li:only-of-type",
            "div:has(> img)",
            "p::before",
        ];
        for selector in needs_the_future {
            let diagnostics = diagnose(&[rule("r", selector)]);
            assert_eq!(
                diagnostics.first().map(|d| d.code),
                Some(pb::SelectorErrorCode::UnsupportedPseudoClassOrElement as i32),
                "expected `{selector}` to be refused as unsupported"
            );
        }
    }

    #[test]
    fn sibling_combinators_are_refused_and_name_the_character() {
        for (selector, character) in [("h1 + p", "+"), ("h1 ~ p", "~")] {
            let diagnostics = diagnose(&[rule("r", selector)]);
            assert_eq!(
                diagnostics.first().map(|d| d.code),
                Some(pb::SelectorErrorCode::UnsupportedCombinator as i32),
                "expected `{selector}` to be refused"
            );
            assert_eq!(diagnostics[0].detail, character);
        }
    }

    #[test]
    fn namespaced_selectors_are_refused_with_their_typed_code() {
        let diagnostics = diagnose(&[rule("ns", "svg|a")]);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].code,
            pb::SelectorErrorCode::NamespacedSelector as i32
        );
    }

    #[test]
    fn diagnose_reports_every_failure_not_just_the_first() {
        let diagnostics = diagnose(&[rule("a", "div >"), rule("b", "p"), rule("c", "")]);
        let ids: Vec<_> = diagnostics.iter().map(|d| d.rule_id.as_str()).collect();
        assert_eq!(ids, ["a", "c"]);
    }

    #[test]
    fn utf16_is_refused_because_lol_html_cannot_tokenize_it() {
        let err = resolve_encoding("utf-16le").unwrap_err();
        assert!(
            err.message().contains("ASCII-compatible"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn an_unset_memory_limit_is_capped_rather_than_infinite() {
        assert_eq!(
            max_memory_bytes(None, usize::MAX),
            DEFAULT_MAX_MEMORY_BYTES as usize
        );
        assert_ne!(max_memory_bytes(None, usize::MAX), usize::MAX);

        // A zero from the wire means "unset", not "no memory allowed".
        let zeroed = pb::MemoryLimits::default();
        assert_eq!(
            max_memory_bytes(Some(&zeroed), usize::MAX),
            DEFAULT_MAX_MEMORY_BYTES as usize
        );
    }

    /// The caller picks a limit, but not one above the server's ceiling, and
    /// an unset limit is the default only when the ceiling allows it.
    #[test]
    fn the_server_ceiling_caps_whatever_limit_the_caller_asks_for() {
        let greedy = with_limits(pb::MemoryLimits {
            max_bytes: u64::MAX,
            ..Default::default()
        });
        assert_eq!(compiled(&greedy, 1 << 20).max_memory_bytes, 1 << 20);

        let modest = with_limits(pb::MemoryLimits {
            max_bytes: 4096,
            ..Default::default()
        });
        assert_eq!(compiled(&modest, 1 << 20).max_memory_bytes, 4096);

        let unset = with_limits(pb::MemoryLimits::default());
        assert_eq!(compiled(&unset, 1 << 20).max_memory_bytes, 1 << 20);
        assert_eq!(
            compiled(&unset, usize::MAX).max_memory_bytes,
            DEFAULT_MAX_MEMORY_BYTES as usize
        );
    }

    /// lol-html charges its preallocation against the same limit, so it is
    /// never allowed past it, including lol-html's own unrequested 1 KiB.
    #[test]
    fn the_preallocation_never_exceeds_the_memory_limit() {
        let oversized = with_limits(pb::MemoryLimits {
            max_bytes: 4096,
            preallocated_buffer_bytes: 1 << 30,
            ..Default::default()
        });
        assert_eq!(
            compiled(&oversized, CEILING).preallocated_buffer_bytes,
            4096
        );

        let tiny_limit = with_limits(pb::MemoryLimits {
            max_bytes: 512,
            ..Default::default()
        });
        assert_eq!(
            compiled(&tiny_limit, CEILING).preallocated_buffer_bytes,
            512
        );

        let ordinary = with_limits(pb::MemoryLimits {
            preallocated_buffer_bytes: 8192,
            ..Default::default()
        });
        assert_eq!(compiled(&ordinary, CEILING).preallocated_buffer_bytes, 8192);
        assert_eq!(
            compiled(&with_limits(pb::MemoryLimits::default()), CEILING).preallocated_buffer_bytes,
            LOL_HTML_PREALLOCATED_BYTES
        );
    }

    #[test]
    fn a_rule_set_over_the_cap_is_refused() {
        let options = pb::ExtractOptions {
            rules: (0..=MAX_RULES).map(|n| rule(&n.to_string(), "p")).collect(),
            ..Default::default()
        };
        let err = rejection(&options);
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("at most 256"), "{}", err.message());

        let at_the_cap = pb::ExtractOptions {
            rules: (0..MAX_RULES).map(|n| rule(&n.to_string(), "p")).collect(),
            ..Default::default()
        };
        assert_eq!(compiled(&at_the_cap, CEILING).rules.len(), MAX_RULES);
    }

    #[test]
    fn an_overlong_selector_or_id_is_refused() {
        let long_selector = pb::ExtractOptions {
            rules: vec![rule(
                "r",
                &format!("div{}", ".c".repeat(MAX_SELECTOR_BYTES)),
            )],
            ..Default::default()
        };
        let err = rejection(&long_selector);
        assert!(err.message().contains("selector of"), "{}", err.message());

        let long_id = pb::ExtractOptions {
            rules: vec![rule(&"i".repeat(MAX_RULE_ID_BYTES + 1), "p")],
            ..Default::default()
        };
        let err = rejection(&long_id);
        assert!(err.message().contains("has an id of"), "{}", err.message());
        assert!(
            !err.message().contains("iiii"),
            "the oversized id is not echoed back: {}",
            err.message()
        );

        let long_document_id = pb::ExtractOptions {
            document_rule: Some(pb::DocumentRule {
                id: "d".repeat(MAX_RULE_ID_BYTES + 1),
                doctype: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = rejection(&long_document_id);
        assert!(
            err.message().contains("document_rule id"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn text_types_default_to_prose_and_titles() {
        assert_eq!(text_types(&[]), DEFAULT_TEXT_TYPES.to_vec());
        // An all-unrecognized list falls back rather than silencing all text.
        assert_eq!(text_types(&[9999]), DEFAULT_TEXT_TYPES.to_vec());
    }

    #[test]
    fn text_types_are_deduplicated() {
        let script = pb::TextType::ScriptData as i32;
        assert_eq!(
            text_types(&[script, script]),
            vec![pb::TextType::ScriptData]
        );
    }
}
