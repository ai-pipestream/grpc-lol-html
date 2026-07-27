// SPDX-License-Identifier: Apache-2.0

//! Typed mappings from lol-html's two error taxonomies onto the wire contract.
//!
//! The two are handled differently, and the difference is forced by lol-html
//! rather than chosen:
//!
//! - [`SelectorError`] is not `#[non_exhaustive]`, so [`selector_code`] is an
//!   exhaustive match. A lol-html upgrade that adds a variant fails the build
//!   here rather than silently reaching clients as `UNSPECIFIED`.
//! - [`RewritingError`] *is* `#[non_exhaustive]` and its own docs require
//!   external matches to carry a wildcard arm, so the same guarantee is not
//!   available. That wildcard is the one path by which an unrecognized
//!   lol-html failure can reach a client as `PARSE_ERROR_CODE_UNSPECIFIED`,
//!   and it always carries lol-html's own message alongside.

use lol_html::errors::{RewritingError, SelectorError};

use crate::proto::v1 as pb;

/// Translate a selector compilation failure into its wire code, plus the
/// offending character for the one variant that names one.
///
/// Three of these are dead in lol-html 3, and are mirrored anyway because the
/// match has to name every variant to stay exhaustive:
///
/// - `EmptyNegation` is deprecated and never constructed.
/// - `NestedNegation` is declared and never constructed either. The input its
///   name describes, `:not(:not(div))`, compiles cleanly.
/// - `UnsupportedSyntax` is reachable only from cssparser's at-rule errors,
///   which parsing a bare selector list never raises, and from two paths
///   lol-html itself marks with `debug_assert!(false)`.
///
/// `every_reachable_selector_error_code_has_a_selector_that_triggers_it` in
/// `tests/extract.rs` carries a selector for each of the other ten, so which
/// are live is recorded rather than remembered.
#[allow(deprecated)]
pub fn selector_code(err: &SelectorError) -> (pb::SelectorErrorCode, String) {
    use pb::SelectorErrorCode as Code;

    match err {
        SelectorError::UnexpectedToken => (Code::UnexpectedToken, String::new()),
        SelectorError::UnexpectedEnd => (Code::UnexpectedEnd, String::new()),
        SelectorError::MissingAttributeName => (Code::MissingAttributeName, String::new()),
        SelectorError::EmptySelector => (Code::EmptySelector, String::new()),
        SelectorError::DanglingCombinator => (Code::DanglingCombinator, String::new()),
        SelectorError::UnexpectedTokenInAttribute => {
            (Code::UnexpectedTokenInAttribute, String::new())
        }
        SelectorError::UnsupportedPseudoClassOrElement => {
            (Code::UnsupportedPseudoClassOrElement, String::new())
        }
        SelectorError::NestedNegation => (Code::NestedNegation, String::new()),
        SelectorError::NamespacedSelector => (Code::NamespacedSelector, String::new()),
        SelectorError::InvalidClassName => (Code::InvalidClassName, String::new()),
        SelectorError::UnsupportedCombinator(c) => (Code::UnsupportedCombinator, c.to_string()),
        SelectorError::UnsupportedSyntax => (Code::UnsupportedSyntax, String::new()),
        // Unreachable: lol-html marks this variant unused and never
        // constructs it. Present so the match stays exhaustive.
        SelectorError::EmptyNegation => (Code::Unspecified, String::new()),
    }
}

/// Build the wire diagnostic for one selector that failed to compile.
pub fn selector_diagnostic(
    rule_id: &str,
    selector: &str,
    err: &SelectorError,
) -> pb::SelectorDiagnostic {
    let (code, detail) = selector_code(err);
    pb::SelectorDiagnostic {
        rule_id: rule_id.to_owned(),
        selector: selector.to_owned(),
        code: code as i32,
        message: err.to_string(),
        detail,
    }
}

/// Translate a parse failure into the terminal in-band error event.
///
/// Deliberately carries no offset. lol-html reports no position for these
/// errors, and the only number we could synthesize is how much had been
/// uploaded when the failure surfaced, which moves with the caller's chunk
/// size and so is not a locator at all.
pub fn stream_error(err: &RewritingError) -> pb::StreamError {
    use pb::ParseErrorCode as Code;

    let code = match err {
        RewritingError::MemoryLimitExceeded(_) => Code::MemoryLimitExceeded,
        RewritingError::ParsingAmbiguity(_) => Code::ParsingAmbiguity,
        RewritingError::ContentHandlerError(_) => Code::ContentHandlerError,
        // `RewritingError` is `#[non_exhaustive]`; this arm is mandatory.
        _ => Code::Unspecified,
    };

    pb::StreamError {
        code: code as i32,
        message: err.to_string(),
    }
}

/// Whether a parse failure is the kind a graceful bail-out converts into a
/// truncated-but-successful run rather than a terminal error.
///
/// Only the memory limit is configurable that way. An ambiguity bail-out is
/// always terminal, because continuing past it is precisely the thing strict
/// mode exists to refuse.
pub const fn is_memory_limit(err: &RewritingError) -> bool {
    matches!(err, RewritingError::MemoryLimitExceeded(_))
}
