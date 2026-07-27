// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests: a real tonic server on an ephemeral port, the generated
//! protobuf client, and real HTML fixtures over a real socket.
//!
//! The load-bearing test here is [`the_event_stream_does_not_depend_on_chunk_size`].
//! Everything this service claims rests on the idea that where the caller
//! happens to split the upload is invisible in the output, and the only way to
//! believe that is to replay every fixture at several chunk sizes and compare
//! the streams byte for byte.

use std::path::PathBuf;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Endpoint, Server};

use grpc_lol_html::LolHtmlGrpc;
use grpc_lol_html::proto::v1 as pb;
use grpc_lol_html::proto::v1::lol_html_service_client::LolHtmlServiceClient;

/// Directory holding the HTML fixtures.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("demos/sample-data")
}

/// Every fixture in the suite with the encoding it is actually written in, so
/// the invariance test cannot silently miss one.
///
/// The encoding matters here: feeding windows-1251 bytes to a UTF-8 parse
/// produces replacement characters, and where the decoder inserts those
/// depends on where the invalid sequence falls relative to a chunk boundary.
/// That is a property of decoding broken input, not of this service, so each
/// fixture is parsed as what it is.
const FIXTURES: &[(&str, &str)] = &[
    ("ambiguity_select_xmp_script.html", ""),
    ("cdata_svg.html", ""),
    ("charset_meta_windows1251.html", "windows-1251"),
    ("deep_nesting.html", ""),
    ("doctype_legacy.html", ""),
    ("duplicate_and_bare_attrs.html", ""),
    ("script_and_style_text.html", ""),
    ("text_split_boundary.html", ""),
    ("unclosed_tags.html", ""),
];

/// Start the server on an ephemeral localhost port and return a connected
/// client.
async fn start_server() -> LolHtmlServiceClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let service = LolHtmlGrpc::new().into_service();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("server failed");
    });
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to server");
    LolHtmlServiceClient::new(channel)
}

/// A rule asking for everything, which is what most tests want.
fn rule(id: &str, selector: &str) -> pb::ExtractRule {
    pb::ExtractRule {
        id: id.to_owned(),
        selector: selector.to_owned(),
        captures: vec![
            pb::Capture::TagName as i32,
            pb::Capture::Attributes as i32,
            pb::Capture::Text as i32,
            pb::Capture::Comments as i32,
            pb::Capture::SourceLocation as i32,
            pb::Capture::EndTag as i32,
        ],
    }
}

/// Options that match every element and ask for everything about it.
fn everything() -> pb::ExtractOptions {
    pb::ExtractOptions {
        rules: vec![rule("all", "*")],
        document_rule: Some(pb::DocumentRule {
            id: "doc".to_owned(),
            doctype: true,
            comments: true,
            text: true,
        }),
        ..Default::default()
    }
}

/// Run one document through `Extract`, uploading it in `chunk_size` slices.
///
/// `chunk_size` of `usize::MAX` sends the whole document in one frame.
async fn extract(
    client: &LolHtmlServiceClient<Channel>,
    bytes: &[u8],
    options: pb::ExtractOptions,
    chunk_size: usize,
) -> Result<Vec<pb::ExtractResponse>, tonic::Status> {
    let mut client = client.clone();

    let mut frames = vec![pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Options(options)),
    }];
    frames.extend(
        bytes
            .chunks(chunk_size.min(bytes.len().max(1)))
            .map(|chunk| pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Chunk(chunk.to_vec())),
            }),
    );

    let mut stream = client
        .extract(tokio_stream::iter(frames))
        .await?
        .into_inner();

    let mut events = Vec::new();
    while let Some(event) = stream.message().await? {
        events.push(event);
    }
    Ok(events)
}

/// Run a fixture file through `Extract`.
async fn extract_file(
    client: &LolHtmlServiceClient<Channel>,
    file: &str,
    options: pb::ExtractOptions,
    chunk_size: usize,
) -> Vec<pb::ExtractResponse> {
    let bytes = std::fs::read(fixtures().join(file)).expect("read fixture");
    extract(client, &bytes, options, chunk_size)
        .await
        .expect("extract should not fail")
}

/// Pull the payloads of one event kind out of a stream.
macro_rules! only {
    ($events:expr, $variant:path) => {
        $events
            .iter()
            .filter_map(|response| match response.event.as_ref() {
                Some($variant(inner)) => Some(inner),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
}

use pb::extract_response::Event;

// ---------------------------------------------------------------------------
// The central claim
// ---------------------------------------------------------------------------

/// Where the caller splits the upload must not be observable in the output.
///
/// This is the whole contract in one test. lol-html hands text to a handler in
/// fragments split at buffer boundaries, and a start tag straddling two chunks
/// is buffered and re-emitted; if any of that leaked through, the streams below
/// would differ. They are compared in full, including byte spans and the final
/// `bytes_parsed`, because all of it is supposed to be chunk-independent.
#[tokio::test]
async fn the_event_stream_does_not_depend_on_chunk_size() {
    let client = start_server().await;

    for (file, encoding) in FIXTURES {
        let bytes = std::fs::read(fixtures().join(file)).expect("read fixture");
        let options = pb::ExtractOptions {
            encoding: (*encoding).to_owned(),
            ..everything()
        };

        // A one-byte chunk size over a 22 KiB fixture is 22k round trips; it
        // proves the same thing on the small ones and keeps the suite quick.
        let sizes: &[usize] = if bytes.len() > 4096 {
            &[64, 1024, usize::MAX]
        } else {
            &[1, 3, 7, 64, 1024, usize::MAX]
        };

        let mut baseline: Option<Vec<pb::ExtractResponse>> = None;
        for &size in sizes {
            let events = extract(&client, &bytes, options.clone(), size)
                .await
                .expect("extract should not fail");
            match &baseline {
                None => baseline = Some(events),
                Some(expected) => assert_eq!(
                    *expected, events,
                    "{file} produced a different stream at chunk size {size}"
                ),
            }
        }

        let events = baseline.expect("at least one chunk size");
        assert!(
            matches!(
                events.first().and_then(|e| e.event.as_ref()),
                Some(Event::Started(_))
            ),
            "{file}: stream must open with `started`"
        );
        assert!(
            matches!(
                events.last().and_then(|e| e.event.as_ref()),
                Some(Event::Finished(_) | Event::Error(_))
            ),
            "{file}: stream must close with `finished` or `error`"
        );
    }
}

/// Events must reach the client before the upload has finished.
///
/// The reason this service is a bidirectional stream rather than an upload
/// followed by a read. The request stream is fed by hand so the test can hold
/// the rest of the document back and still demand a match.
#[tokio::test]
async fn matches_arrive_before_the_upload_is_finished() {
    let mut client = start_server().await;
    let (tx, rx) = tokio::sync::mpsc::channel(8);

    // Queued before the call is awaited, deliberately. The server validates
    // options before opening the response stream, so it emits no response
    // headers until it has them, and awaiting the call first would deadlock.
    // The proto says so on the RPC; this is the test that keeps it true.
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Options(pb::ExtractOptions {
            rules: vec![rule("h1", "h1")],
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Chunk(
            b"<html><body><h1>Early</h1>".to_vec(),
        )),
    })
    .await
    .unwrap();

    let mut stream = client
        .extract(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("open the call")
        .into_inner();

    // Nothing else has been sent and the request stream is still open, so if
    // the server buffered the document this would block until the test's
    // timeout rather than return.
    let started = stream.message().await.unwrap().unwrap();
    assert!(matches!(started.event, Some(Event::Started(_))));

    let matched = stream.message().await.unwrap().unwrap();
    let Some(Event::Element(element)) = matched.event else {
        panic!("expected the h1 before the document ended, got {matched:?}");
    };
    assert_eq!(element.tag_name, "h1");

    // Only now finish the document.
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Chunk(
            b"<p>Late</p></body></html>".to_vec(),
        )),
    })
    .await
    .unwrap();
    drop(tx);

    let mut saw_finished = false;
    while let Some(event) = stream.message().await.unwrap() {
        if matches!(event.event, Some(Event::Finished(_))) {
            saw_finished = true;
        }
    }
    assert!(saw_finished, "stream should close with `finished`");
}

// ---------------------------------------------------------------------------
// Hazard 1: text chunks are not text nodes
// ---------------------------------------------------------------------------

/// A text node split across chunks arrives as one reassembled event.
#[tokio::test]
async fn text_nodes_are_reassembled_across_chunk_boundaries() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "p".to_owned(),
            selector: "p#long".to_owned(),
            captures: vec![pb::Capture::Text as i32],
        }],
        ..Default::default()
    };

    let events = extract_file(&client, "text_split_boundary.html", options, 7).await;
    let texts = only!(events, Event::Text);

    assert_eq!(
        texts.len(),
        1,
        "the paragraph is one text node, not several"
    );
    assert!(texts[0].text.starts_with("Antidisestablishmentarianism"));
    assert!(texts[0].text.ends_with("done with it."));
    assert!(texts[0].last_in_node);
}

/// `raw_text_chunks` hands back the fragments, and concatenating them in order
/// reproduces the reassembled text exactly.
#[tokio::test]
async fn raw_text_chunks_reassemble_into_the_default_output() {
    let client = start_server().await;
    let base = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "p".to_owned(),
            selector: "p#long".to_owned(),
            captures: vec![pb::Capture::Text as i32],
        }],
        ..Default::default()
    };

    let coalesced = extract_file(&client, "text_split_boundary.html", base.clone(), 7).await;
    let raw = extract_file(
        &client,
        "text_split_boundary.html",
        pb::ExtractOptions {
            raw_text_chunks: true,
            ..base
        },
        7,
    )
    .await;

    let whole = only!(coalesced, Event::Text);
    let pieces = only!(raw, Event::Text);

    assert!(
        pieces.len() > whole.len(),
        "at a 7 byte chunk size the raw view should be more fragmented"
    );
    let rejoined: String = pieces.iter().map(|piece| piece.text.as_str()).collect();
    assert_eq!(rejoined, whole[0].text);
    assert!(
        pieces.last().unwrap().last_in_node,
        "the final fragment must be marked, or reassembly has no boundary"
    );
}

// ---------------------------------------------------------------------------
// Hazard 2: strict-mode ambiguity bail-out
// ---------------------------------------------------------------------------

/// Ambiguous markup ends the stream with a typed in-band error, not a crash.
#[tokio::test]
async fn ambiguous_markup_ends_the_stream_with_a_typed_error() {
    let client = start_server().await;
    let events = extract_file(
        &client,
        "ambiguity_select_xmp_script.html",
        everything(),
        64,
    )
    .await;

    let errors = only!(events, Event::Error);
    assert_eq!(errors.len(), 1, "expected exactly one terminal error");
    assert_eq!(
        errors[0].code,
        pb::ParseErrorCode::ParsingAmbiguity as i32,
        "{}",
        errors[0].message
    );

    // The error is terminal, and everything before it still stands.
    assert!(matches!(
        events.last().and_then(|e| e.event.as_ref()),
        Some(Event::Error(_))
    ));
    let elements = only!(events, Event::Element);
    assert!(
        elements.iter().any(|el| el.tag_name == "p"),
        "events emitted before the bail-out are valid and should be delivered"
    );
}

/// Opting in to guessing lets the same document through.
///
/// Also pins the direction of the flag: the safe behaviour is the proto3 zero
/// value, so a client that never heard of this field gets the bail-out.
#[tokio::test]
async fn allowing_ambiguous_markup_parses_what_the_default_refuses() {
    let client = start_server().await;
    let events = extract_file(
        &client,
        "ambiguity_select_xmp_script.html",
        pb::ExtractOptions {
            allow_ambiguous_markup: true,
            ..everything()
        },
        64,
    )
    .await;

    assert!(only!(events, Event::Error).is_empty());
    assert!(matches!(
        events.last().and_then(|e| e.event.as_ref()),
        Some(Event::Finished(_))
    ));
}

// ---------------------------------------------------------------------------
// Hazard 3: memory limits
// ---------------------------------------------------------------------------

/// A document that outgrows its memory cap fails in-band, and the server keeps
/// serving.
///
/// The surviving half is the point. An unbounded parser that dies takes every
/// other in-flight request with it, so the test proves the process is still
/// answering afterwards rather than merely that one call failed.
#[tokio::test]
async fn a_document_over_its_memory_cap_fails_in_band_and_the_server_survives() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        limits: Some(pb::MemoryLimits {
            max_bytes: 4096,
            graceful_bail_out: false,
            preallocated_buffer_bytes: 0,
        }),
        ..everything()
    };

    let events = extract_file(&client, "deep_nesting.html", options, 1024).await;
    let errors = only!(events, Event::Error);
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].code,
        pb::ParseErrorCode::MemoryLimitExceeded as i32,
        "{}",
        errors[0].message
    );

    // Same process, same connection: a well-behaved document still works.
    let after = extract_file(&client, "doctype_legacy.html", everything(), 64).await;
    assert!(matches!(
        after.last().and_then(|e| e.event.as_ref()),
        Some(Event::Finished(_))
    ));
}

/// With `graceful_bail_out` the same overrun becomes a truncated success.
#[tokio::test]
async fn a_graceful_bail_out_is_a_truncated_success_not_an_error() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        limits: Some(pb::MemoryLimits {
            max_bytes: 4096,
            graceful_bail_out: true,
            preallocated_buffer_bytes: 0,
        }),
        ..everything()
    };

    let events = extract_file(&client, "deep_nesting.html", options, 1024).await;
    assert!(only!(events, Event::Error).is_empty());

    let finished = only!(events, Event::Finished);
    assert_eq!(finished.len(), 1);
    assert!(
        finished[0].bailed_out,
        "a truncated run must say so rather than look complete"
    );
    assert!(!finished[0].bail_out_reason.is_empty());
}

// ---------------------------------------------------------------------------
// Hazard 4: not all text is prose
// ---------------------------------------------------------------------------

/// By default, script and style contents are not reported as text.
#[tokio::test]
async fn script_and_style_text_is_not_prose_by_default() {
    let client = start_server().await;
    let events = extract_file(&client, "script_and_style_text.html", everything(), 64).await;

    let texts = only!(events, Event::Text);
    let joined: String = texts.iter().map(|t| t.text.as_str()).collect();

    assert!(joined.contains("only real prose"));
    assert!(
        joined.contains("Every text type in one document"),
        "the title is RCDATA and is wanted by default"
    );
    assert!(
        !joined.contains("notProse"),
        "script contents must not arrive as prose: {joined}"
    );
    assert!(
        !joined.contains("#333"),
        "style contents must not arrive as prose: {joined}"
    );

    for text in &texts {
        assert!(
            text.text_type == pb::TextType::Data as i32
                || text.text_type == pb::TextType::Rcdata as i32,
            "unexpected text type {} for {:?}",
            text.text_type,
            text.text
        );
    }
}

/// Script text is available, but only by asking for it.
#[tokio::test]
async fn script_text_arrives_when_it_is_asked_for() {
    let client = start_server().await;
    let events = extract_file(
        &client,
        "script_and_style_text.html",
        pb::ExtractOptions {
            text_types: vec![pb::TextType::ScriptData as i32],
            ..everything()
        },
        64,
    )
    .await;

    let texts = only!(events, Event::Text);
    assert!(!texts.is_empty());
    for text in &texts {
        assert_eq!(text.text_type, pb::TextType::ScriptData as i32);
    }
    let joined: String = texts.iter().map(|t| t.text.as_str()).collect();
    assert!(joined.contains("notProse"));
    assert!(
        !joined.contains("only real prose"),
        "asking for script text should not also hand back prose"
    );
}

// ---------------------------------------------------------------------------
// The rest of the surface
// ---------------------------------------------------------------------------

/// Namespaces survive: an `<a>` in SVG is not an `<a>` in HTML.
#[tokio::test]
async fn foreign_content_reports_its_own_namespace() {
    let client = start_server().await;
    let events = extract_file(&client, "cdata_svg.html", everything(), 64).await;

    let anchors: Vec<_> = only!(events, Event::Element)
        .into_iter()
        .filter(|el| el.tag_name == "a")
        .collect();
    assert_eq!(anchors.len(), 2, "one anchor in SVG, one in HTML");

    let namespaces: Vec<i32> = anchors.iter().map(|el| el.namespace).collect();
    assert!(namespaces.contains(&(pb::Namespace::Svg as i32)));
    assert!(namespaces.contains(&(pb::Namespace::Html as i32)));
}

/// A legacy doctype carries its public and system identifiers.
#[tokio::test]
async fn a_legacy_doctype_reports_its_identifiers() {
    let client = start_server().await;
    let events = extract_file(&client, "doctype_legacy.html", everything(), 64).await;

    let doctypes = only!(events, Event::Doctype);
    assert_eq!(doctypes.len(), 1);
    assert_eq!(doctypes[0].rule_id, "doc");
    assert_eq!(doctypes[0].name.as_deref(), Some("html"));
    assert_eq!(
        doctypes[0].public_id.as_deref(),
        Some("-//W3C//DTD XHTML 1.0 Transitional//EN")
    );
    assert!(
        doctypes[0]
            .system_id
            .as_deref()
            .is_some_and(|id| id.ends_with("xhtml1-transitional.dtd"))
    );
}

/// Attribute case is preserved alongside the normalized name, bare attributes
/// come through with an empty value, and spans point at real bytes.
#[tokio::test]
async fn attributes_keep_their_case_their_bare_forms_and_their_spans() {
    let client = start_server().await;
    let file = "duplicate_and_bare_attrs.html";
    let source = std::fs::read_to_string(fixtures().join(file)).unwrap();
    let events = extract_file(&client, file, everything(), 64).await;

    let script = only!(events, Event::Element)
        .into_iter()
        .find(|el| el.tag_name == "script")
        .expect("the script element");

    let by_name: Vec<(&str, &str)> = script
        .attributes
        .iter()
        .map(|attr| (attr.name.as_str(), attr.value.as_str()))
        .collect();
    assert_eq!(
        by_name,
        [("src", "/a.js"), ("defer", ""), ("async", "")],
        "names normalize to lowercase and bare attributes carry an empty value"
    );

    let src = &script.attributes[0];
    assert_eq!(src.name_raw, "SRC", "the written case is preserved");

    // Spans index the document, so slicing the source with them must give the
    // text back.
    let name_span = src.name_span.as_ref().expect("a name span");
    assert_eq!(
        &source[name_span.start as usize..name_span.end as usize],
        "SRC"
    );
    let value_span = src.value_span.as_ref().expect("a value span");
    assert_eq!(
        &source[value_span.start as usize..value_span.end as usize],
        "/a.js",
        "the value span covers the value itself, not the surrounding quotes"
    );

    // A bare attribute carries neither span: lol-html records one position
    // for the name/value pair, and a bare attribute is not one.
    let defer = &script.attributes[1];
    assert_eq!(defer.name, "defer");
    assert!(defer.name_span.is_none());
    assert!(defer.value_span.is_none());
}

/// End tags are reported for elements that close, and not for those that
/// cannot or simply do not.
#[tokio::test]
async fn end_tags_are_reported_only_where_they_exist() {
    let client = start_server().await;

    let events = extract_file(&client, "duplicate_and_bare_attrs.html", everything(), 64).await;
    let ends: Vec<&str> = only!(events, Event::EndTag)
        .iter()
        .map(|end| end.name.as_str())
        .collect();
    assert!(ends.contains(&"script"));
    assert!(
        !ends.contains(&"input"),
        "a void element never produces an end tag"
    );

    // A void element says as much up front, which is the signal to use.
    let input = only!(events, Event::Element)
        .into_iter()
        .find(|el| el.tag_name == "input")
        .expect("the input element");
    assert!(!input.can_have_content);

    // An element the document never closes produces no end tag either. This
    // is why `end_tag` is not a dependable "element finished" signal.
    let unclosed = extract_file(&client, "unclosed_tags.html", everything(), 64).await;
    let unclosed_ends: Vec<&str> = only!(unclosed, Event::EndTag)
        .iter()
        .map(|end| end.name.as_str())
        .collect();
    assert!(
        !unclosed_ends.contains(&"span"),
        "the span is never closed, so there is no end tag to report"
    );
    assert!(matches!(
        unclosed.last().and_then(|e| e.event.as_ref()),
        Some(Event::Finished(_))
    ));
}

/// A `<meta charset>` can redirect decoding mid-document.
#[tokio::test]
async fn a_meta_charset_can_override_the_declared_encoding() {
    let client = start_server().await;
    let file = "charset_meta_windows1251.html";

    let adjusted = extract_file(
        &client,
        file,
        pb::ExtractOptions {
            adjust_charset_on_meta_tag: true,
            ..everything()
        },
        64,
    )
    .await;
    let joined: String = only!(adjusted, Event::Text)
        .iter()
        .map(|t| t.text.as_str())
        .collect();
    assert!(
        joined.contains("Привет, мир"),
        "windows-1251 bytes should decode once the meta tag is honoured: {joined:?}"
    );

    // Without it the same bytes are read as UTF-8 and come out as mojibake,
    // which is the failure mode the option exists to prevent.
    let unadjusted = extract_file(&client, file, everything(), 64).await;
    let raw: String = only!(unadjusted, Event::Text)
        .iter()
        .map(|t| t.text.as_str())
        .collect();
    assert!(!raw.contains("Привет, мир"));
}

/// Explicitly naming the encoding works too, and without needing the meta tag.
#[tokio::test]
async fn an_explicit_encoding_label_is_honoured() {
    let client = start_server().await;
    let events = extract_file(
        &client,
        "charset_meta_windows1251.html",
        pb::ExtractOptions {
            encoding: "windows-1251".to_owned(),
            ..everything()
        },
        64,
    )
    .await;

    let started = only!(events, Event::Started);
    assert_eq!(started[0].encoding, "windows-1251");
    let joined: String = only!(events, Event::Text)
        .iter()
        .map(|t| t.text.as_str())
        .collect();
    assert!(joined.contains("Привет, мир"));
}

/// UTF-16 is refused up front, with a reason.
#[tokio::test]
async fn utf16_is_refused_before_the_upload_starts() {
    let client = start_server().await;
    let bytes = std::fs::read(fixtures().join("utf16.html")).unwrap();
    let err = extract(
        &client,
        &bytes,
        pb::ExtractOptions {
            encoding: "utf-16le".to_owned(),
            ..everything()
        },
        64,
    )
    .await
    .expect_err("utf-16 should be refused");

    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("ASCII-compatible"),
        "{}",
        err.message()
    );
}

/// `ValidateSelectors` reports every failure and stays quiet about the rest.
#[tokio::test]
async fn validate_selectors_reports_the_bad_ones_only() {
    let mut client = start_server().await;
    let response = client
        .validate_selectors(pb::ValidateSelectorsRequest {
            rules: vec![
                rule("good", "div.content > p"),
                rule("future", "li:last-child"),
                rule("sibling", "h1 + p"),
                rule("also_good", "a[href^='https']"),
                rule("namespaced", "svg|a"),
            ],
        })
        .await
        .expect("validate")
        .into_inner();

    let ids: Vec<&str> = response
        .diagnostics
        .iter()
        .map(|d| d.rule_id.as_str())
        .collect();
    assert_eq!(ids, ["future", "sibling", "namespaced"]);

    assert_eq!(
        response.diagnostics[0].code,
        pb::SelectorErrorCode::UnsupportedPseudoClassOrElement as i32
    );
    assert_eq!(
        response.diagnostics[1].code,
        pb::SelectorErrorCode::UnsupportedCombinator as i32
    );
    assert_eq!(response.diagnostics[1].detail, "+");
    assert_eq!(
        response.diagnostics[2].code,
        pb::SelectorErrorCode::NamespacedSelector as i32
    );
}

/// Per-rule counts are reported for every rule, including the ones that
/// matched nothing.
#[tokio::test]
async fn finished_counts_every_rule_including_the_empty_ones() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        rules: vec![
            pb::ExtractRule {
                captures: vec![pb::Capture::TagName as i32],
                ..rule("paragraphs", "p")
            },
            pb::ExtractRule {
                captures: vec![pb::Capture::TagName as i32],
                ..rule("tables", "table")
            },
        ],
        ..Default::default()
    };

    let events = extract_file(&client, "text_split_boundary.html", options, 64).await;
    let finished = only!(events, Event::Finished);
    assert_eq!(finished[0].matches_by_rule.get("paragraphs"), Some(&2));
    assert_eq!(
        finished[0].matches_by_rule.get("tables"),
        Some(&0),
        "a rule that matched nothing must be distinguishable from a missing rule"
    );
}

/// The first frame has to be the options, and the options only come once.
#[tokio::test]
async fn the_options_frame_is_required_first_and_only_once() {
    let mut client = start_server().await;

    // A chunk before any options.
    let err = client
        .extract(tokio_stream::iter(vec![pb::ExtractRequest {
            frame: Some(pb::extract_request::Frame::Chunk(b"<p>x</p>".to_vec())),
        }]))
        .await
        .expect_err("a leading chunk should be refused");
    assert_eq!(err.code(), Code::InvalidArgument);

    // Options twice.
    let frames = vec![
        pb::ExtractRequest {
            frame: Some(pb::extract_request::Frame::Options(everything())),
        },
        pb::ExtractRequest {
            frame: Some(pb::extract_request::Frame::Options(everything())),
        },
    ];
    let mut stream = client
        .extract(tokio_stream::iter(frames))
        .await
        .expect("the call itself opens")
        .into_inner();

    let mut error = None;
    loop {
        match stream.message().await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(status) => {
                error = Some(status);
                break;
            }
        }
    }
    assert_eq!(
        error.map(|status| status.code()),
        Some(Code::InvalidArgument),
        "a second options frame should end the call"
    );
}

/// A request that asks for nothing is refused rather than silently returning
/// an empty stream.
#[tokio::test]
async fn a_request_with_no_rules_is_refused() {
    let client = start_server().await;
    let err = extract(&client, b"<p>x</p>", pb::ExtractOptions::default(), 64)
        .await
        .expect_err("an empty rule set should be refused");
    assert_eq!(err.code(), Code::InvalidArgument);
}

/// Every `Capture` does something observable, so none can quietly become a
/// no-op.
#[tokio::test]
async fn every_capture_has_a_visible_effect() {
    let client = start_server().await;
    let html = b"<div id=\"d\"><!-- note -->text</div>";

    let with = |captures: Vec<i32>| pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "r".to_owned(),
            selector: "div".to_owned(),
            captures,
        }],
        ..Default::default()
    };

    let tag = extract(&client, html, with(vec![pb::Capture::TagName as i32]), 64)
        .await
        .unwrap();
    assert_eq!(only!(tag, Event::Element)[0].tag_name, "div");

    let attrs = extract(
        &client,
        html,
        with(vec![pb::Capture::Attributes as i32]),
        64,
    )
    .await
    .unwrap();
    assert_eq!(only!(attrs, Event::Element)[0].attributes.len(), 1);
    assert!(
        only!(tag, Event::Element)[0].attributes.is_empty(),
        "attributes must not arrive unless asked for"
    );

    let text = extract(&client, html, with(vec![pb::Capture::Text as i32]), 64)
        .await
        .unwrap();
    assert_eq!(only!(text, Event::Text)[0].text, "text");

    let comments = extract(&client, html, with(vec![pb::Capture::Comments as i32]), 64)
        .await
        .unwrap();
    assert_eq!(only!(comments, Event::Comment)[0].text, " note ");

    let spans = extract(
        &client,
        html,
        with(vec![
            pb::Capture::TagName as i32,
            pb::Capture::SourceLocation as i32,
        ]),
        64,
    )
    .await
    .unwrap();
    assert!(only!(spans, Event::Element)[0].span.is_some());
    assert!(
        only!(tag, Event::Element)[0].span.is_none(),
        "spans must not arrive unless asked for"
    );

    let ends = extract(&client, html, with(vec![pb::Capture::EndTag as i32]), 64)
        .await
        .unwrap();
    assert_eq!(only!(ends, Event::EndTag)[0].name, "div");
    assert!(
        only!(tag, Event::EndTag).is_empty(),
        "end tags must not arrive unless asked for"
    );
}
