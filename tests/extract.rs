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
    ("entities.html", ""),
    ("json_ld_product.html", ""),
    ("mathml_formula.html", ""),
    ("plaintext_tail.html", ""),
    ("spa_shell.html", ""),
    ("script_and_style_text.html", ""),
    ("text_split_boundary.html", ""),
    ("unclosed_tags.html", ""),
];

/// Start the server on an ephemeral localhost port and return a connected
/// client.
async fn start_server() -> LolHtmlServiceClient<Channel> {
    start_configured_server(LolHtmlGrpc::new().into_service()).await
}

/// Start a pre-configured service on an ephemeral localhost port and return a
/// connected client.
async fn start_configured_server(
    service: pb::lol_html_service_server::LolHtmlServiceServer<LolHtmlGrpc>,
) -> LolHtmlServiceClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
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
// Hazard: lol-html hands text and attributes back exactly as written, so the
// entity decode the contract promises is the server's job
// ---------------------------------------------------------------------------

/// DATA and RCDATA text arrives entity decoded. The 7-byte chunk size splits
/// `&amp;` across fragments, so this also pins that decoding happens after
/// reassembly rather than per fragment.
#[tokio::test]
async fn entities_are_decoded_in_data_and_rcdata_text() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        rules: vec![
            pb::ExtractRule {
                id: "case".to_owned(),
                selector: "p#case".to_owned(),
                captures: vec![pb::Capture::Text as i32],
            },
            pb::ExtractRule {
                id: "title".to_owned(),
                selector: "title".to_owned(),
                captures: vec![pb::Capture::Text as i32],
            },
        ],
        ..Default::default()
    };

    let events = extract_file(&client, "entities.html", options, 7).await;
    let texts = only!(events, Event::Text);

    let case = texts
        .iter()
        .find(|t| t.rule_id == "case")
        .expect("p#case text");
    assert_eq!(
        case.text,
        "Byrd & Davis, “curly” quotes, <escaped tag>, café."
    );

    let title = texts
        .iter()
        .find(|t| t.rule_id == "title")
        .expect("title text");
    assert_eq!(title.text, "Baughman & Datron");
}

/// Script text is never entity decoded: `&amp;&amp;` inside a script means
/// those ten characters, not `&&`.
#[tokio::test]
async fn script_text_is_not_entity_decoded() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "js".to_owned(),
            selector: "script".to_owned(),
            captures: vec![pb::Capture::Text as i32],
        }],
        text_types: vec![pb::TextType::ScriptData as i32],
        ..Default::default()
    };

    let events = extract_file(&client, "entities.html", options, 7).await;
    let texts = only!(events, Event::Text);
    assert_eq!(texts.len(), 1);
    assert_eq!(texts[0].text, "if (a &amp;&amp; b) { run(); }");
}

/// Attribute values decode with attribute-context rules: `&amp;` becomes
/// `&`, but the legacy semicolon-less `&amp=` stays literal because the
/// spec keeps `?a=1&amp=2`-style query strings intact.
#[tokio::test]
async fn attribute_values_are_entity_decoded() {
    let client = start_server().await;
    let options = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "a".to_owned(),
            selector: "a".to_owned(),
            captures: vec![pb::Capture::Attributes as i32],
        }],
        ..Default::default()
    };

    let events = extract_file(&client, "entities.html", options, 7).await;
    let elements = only!(events, Event::Element);
    assert_eq!(elements.len(), 1);

    let value = |name: &str| {
        elements[0]
            .attributes
            .iter()
            .find(|a| a.name == name)
            .unwrap_or_else(|| panic!("attribute {name}"))
            .value
            .clone()
    };
    assert_eq!(value("href"), "x?a=1&amp=2&b=3");
    assert_eq!(value("title"), "A & B");
}

/// Raw fragments are verbatim — no decoding — and the documented recipe
/// (reassemble in order, then entity-decode) reproduces the default output.
#[tokio::test]
async fn raw_text_fragments_are_verbatim_and_decode_after_reassembly() {
    let client = start_server().await;
    let base = pb::ExtractOptions {
        rules: vec![pb::ExtractRule {
            id: "case".to_owned(),
            selector: "p#case".to_owned(),
            captures: vec![pb::Capture::Text as i32],
        }],
        ..Default::default()
    };

    let coalesced = extract_file(&client, "entities.html", base.clone(), 3).await;
    let raw = extract_file(
        &client,
        "entities.html",
        pb::ExtractOptions {
            raw_text_chunks: true,
            ..base
        },
        3,
    )
    .await;

    let whole = only!(coalesced, Event::Text);
    let pieces = only!(raw, Event::Text);

    let rejoined: String = pieces.iter().map(|piece| piece.text.as_str()).collect();
    assert!(
        rejoined.contains("&amp;"),
        "raw fragments must be verbatim, got {rejoined:?}"
    );
    assert_eq!(htmlize::unescape(&rejoined), whole[0].text);
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

// ---------------------------------------------------------------------------
// Exhaustiveness: every variant in the contract, produced by real input
// ---------------------------------------------------------------------------
//
// `convert::text_type` and `errors::selector_code` are exhaustive matches over
// lol-html's own enums, so a library upgrade that *adds* a variant is a compile
// error rather than an `UNSPECIFIED` quietly reaching clients. These tests are
// the other half: proof that every variant the contract declares is actually
// produced by real input, and a record of what produces it.
//
// Each is driven by an exhaustive match on the wire enum, so declaring a
// variant in the proto without giving it a source is also a compile error, and
// each checks that the contract has no variant numbered past the ones named
// here, which is the same guarantee for the list itself.

/// Options that match everything and ask for every kind of text.
fn every_text_type() -> pb::ExtractOptions {
    pb::ExtractOptions {
        text_types: ALL_TEXT_TYPES
            .iter()
            .map(|text_type| *text_type as i32)
            .collect(),
        ..everything()
    }
}

const ALL_TEXT_TYPES: [pb::TextType; 7] = [
    pb::TextType::Unspecified,
    pb::TextType::Data,
    pb::TextType::Rcdata,
    pb::TextType::RawText,
    pb::TextType::ScriptData,
    pb::TextType::PlainText,
    pb::TextType::CdataSection,
];

/// The fixture that produces each text type, and the markup in it that does.
const fn text_type_source(text_type: pb::TextType) -> Option<(&'static str, &'static str)> {
    use pb::TextType as T;
    match text_type {
        T::Unspecified => None,
        T::Data => Some(("script_and_style_text.html", "<p>")),
        T::Rcdata => Some(("script_and_style_text.html", "<title> and <textarea>")),
        T::RawText => Some(("script_and_style_text.html", "<style> and <noscript>")),
        T::ScriptData => Some(("script_and_style_text.html", "<script>")),
        T::PlainText => Some(("plaintext_tail.html", "<plaintext>")),
        T::CdataSection => Some(("cdata_svg.html", "<![CDATA[ ]]> inside <svg>")),
    }
}

/// Every text type is reachable from a committed fixture, and `UNSPECIFIED` is
/// not reachable at all.
#[tokio::test]
async fn every_text_type_is_produced_by_a_fixture() {
    assert!(
        pb::TextType::try_from(ALL_TEXT_TYPES.len() as i32).is_err(),
        "the contract grew a text type this list does not name"
    );

    let client = start_server().await;

    for text_type in ALL_TEXT_TYPES {
        let Some((fixture, markup)) = text_type_source(text_type) else {
            continue;
        };
        let events = extract_file(&client, fixture, every_text_type(), 64).await;
        let seen: Vec<i32> = only!(events, Event::Text)
            .iter()
            .map(|text| text.text_type)
            .collect();
        assert!(
            seen.contains(&(text_type as i32)),
            "{text_type:?} should come from {markup} in {fixture}, saw {seen:?}"
        );
        assert!(
            !seen.contains(&(pb::TextType::Unspecified as i32)),
            "{fixture} produced an unspecified text type"
        );
    }
}

const ALL_NAMESPACES: [pb::Namespace; 4] = [
    pb::Namespace::Unspecified,
    pb::Namespace::Html,
    pb::Namespace::Svg,
    pb::Namespace::Mathml,
];

/// The fixture that produces each namespace, and the element in it that does.
const fn namespace_source(namespace: pb::Namespace) -> Option<(&'static str, &'static str)> {
    use pb::Namespace as N;
    match namespace {
        N::Unspecified => None,
        N::Html => Some(("cdata_svg.html", "<p>")),
        N::Svg => Some(("cdata_svg.html", "<svg>")),
        N::Mathml => Some(("mathml_formula.html", "<math>")),
    }
}

/// Every namespace is reachable from a committed fixture.
///
/// `convert::namespace` matches on a URI string rather than an enum, so unlike
/// the others it cannot be exhaustive and a namespace nobody exercises would
/// stay unnoticed. MathML is the one that was: the URI constant and its unit
/// test existed from the first commit, and no document had ever reached them.
#[tokio::test]
async fn every_namespace_is_produced_by_a_fixture() {
    assert!(
        pb::Namespace::try_from(ALL_NAMESPACES.len() as i32).is_err(),
        "the contract grew a namespace this list does not name"
    );

    let client = start_server().await;

    for namespace in ALL_NAMESPACES {
        let Some((fixture, element)) = namespace_source(namespace) else {
            continue;
        };
        let events = extract_file(&client, fixture, everything(), 64).await;
        let seen: Vec<i32> = only!(events, Event::Element)
            .iter()
            .map(|element| element.namespace)
            .collect();
        assert!(
            seen.contains(&(namespace as i32)),
            "{namespace:?} should come from {element} in {fixture}, saw {seen:?}"
        );
        assert!(
            !seen.contains(&(pb::Namespace::Unspecified as i32)),
            "{fixture} produced an unspecified namespace"
        );
    }
}

const ALL_SELECTOR_ERROR_CODES: [pb::SelectorErrorCode; 13] = [
    pb::SelectorErrorCode::Unspecified,
    pb::SelectorErrorCode::UnexpectedToken,
    pb::SelectorErrorCode::UnexpectedEnd,
    pb::SelectorErrorCode::MissingAttributeName,
    pb::SelectorErrorCode::EmptySelector,
    pb::SelectorErrorCode::DanglingCombinator,
    pb::SelectorErrorCode::UnexpectedTokenInAttribute,
    pb::SelectorErrorCode::UnsupportedPseudoClassOrElement,
    pb::SelectorErrorCode::NestedNegation,
    pb::SelectorErrorCode::NamespacedSelector,
    pb::SelectorErrorCode::InvalidClassName,
    pb::SelectorErrorCode::UnsupportedCombinator,
    pb::SelectorErrorCode::UnsupportedSyntax,
];

/// A selector that triggers each code, or `None` where lol-html 3 has no path
/// to it.
///
/// Two are dead upstream, and both are mirrored anyway because the match in
/// `errors::selector_code` has to name every variant to stay exhaustive:
///
/// - `NESTED_NEGATION` is declared in lol-html's error enum and never
///   constructed anywhere in its source. The input the name describes,
///   `:not(:not(div))`, compiles cleanly, which the test below pins.
/// - `UNSUPPORTED_SYNTAX` is reachable only from cssparser's at-rule errors,
///   which parsing a bare selector list never raises, and from two paths
///   lol-html marks with `debug_assert!(false)` as unreachable.
const fn selector_triggering(code: pb::SelectorErrorCode) -> Option<&'static str> {
    use pb::SelectorErrorCode as C;
    match code {
        C::Unspecified | C::NestedNegation | C::UnsupportedSyntax => None,
        C::UnexpectedToken => Some("div@"),
        C::UnexpectedEnd => Some("div."),
        C::MissingAttributeName => Some(r#"div[="foo"]"#),
        C::EmptySelector => Some(""),
        C::DanglingCombinator => Some("div >"),
        C::UnexpectedTokenInAttribute => Some(r#"div[foo~"bar"]"#),
        C::UnsupportedPseudoClassOrElement => Some("li:last-child"),
        C::NamespacedSelector => Some("svg|a"),
        C::InvalidClassName => Some(".foo()"),
        C::UnsupportedCombinator => Some("h1 + p"),
    }
}

/// Every selector error code lol-html can reach has a selector that reaches it.
#[tokio::test]
async fn every_reachable_selector_error_code_has_a_selector_that_triggers_it() {
    assert!(
        pb::SelectorErrorCode::try_from(ALL_SELECTOR_ERROR_CODES.len() as i32).is_err(),
        "the contract grew a selector error code this list does not name"
    );

    let expected: Vec<(pb::SelectorErrorCode, &str)> = ALL_SELECTOR_ERROR_CODES
        .iter()
        .filter_map(|code| selector_triggering(*code).map(|selector| (*code, selector)))
        .collect();

    let mut client = start_server().await;
    let response = client
        .validate_selectors(pb::ValidateSelectorsRequest {
            rules: expected
                .iter()
                .map(|(code, selector)| rule(code.as_str_name(), selector))
                .collect(),
        })
        .await
        .expect("validate")
        .into_inner();

    // Every one of them fails, so the diagnostics line up with the rules.
    assert_eq!(
        response.diagnostics.len(),
        expected.len(),
        "every selector here is supposed to be rejected"
    );
    for ((code, selector), diagnostic) in expected.iter().zip(&response.diagnostics) {
        assert_eq!(
            diagnostic.code,
            *code as i32,
            "`{selector}` should be {code:?}, got {:?}",
            pb::SelectorErrorCode::try_from(diagnostic.code)
        );
    }

    // The one input `NESTED_NEGATION` names, which lol-html accepts.
    let nested = client
        .validate_selectors(pb::ValidateSelectorsRequest {
            rules: vec![rule("nested", ":not(:not(div))")],
        })
        .await
        .expect("validate")
        .into_inner();
    assert!(
        nested.diagnostics.is_empty(),
        "a nested negation compiles in lol-html 3, which is why NESTED_NEGATION is dead: {:?}",
        nested.diagnostics
    );
}

/// The documented chunk cap has to be the one that actually fires.
///
/// tonic enforces its own decoding limit before any handler runs. If that
/// limit sits at or below the server's cap, a chunk over the cap is refused by
/// the transport with `OutOfRange` and a sentence about decoded message
/// lengths, and the caller never sees the `INVALID_ARGUMENT` that names the
/// limit and says to split the document.
///
/// Run against a deliberately tiny cap so the test costs kilobytes rather than
/// the 100 MiB the real default would need. The invariant is the same at any
/// size: overshoot the cap and our error is the one that speaks.
#[tokio::test]
async fn an_oversized_chunk_is_refused_by_the_server_not_the_transport() {
    let client = start_configured_server(
        LolHtmlGrpc::new()
            .with_max_chunk_bytes(64 * 1024)
            .into_service(),
    )
    .await;

    // Over the 64 KiB cap, under the 128 KiB transport backstop.
    let oversized = vec![b'x'; 96 * 1024];
    let err = extract(&client, &oversized, everything(), usize::MAX)
        .await
        .expect_err("a chunk over the cap should be refused");

    assert_eq!(
        err.code(),
        Code::InvalidArgument,
        "expected the server's own limit to fire, got {err:?}"
    );
    assert!(
        err.message().contains("exceeds") && err.message().contains("smaller chunks"),
        "the error should say what to do: {}",
        err.message()
    );
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// A client that asks for zstd gets a working stream; one that sends
/// zstd-compressed frames is understood. If either direction were unwired the
/// round trip would fail at the transport with `Unimplemented`.
#[tokio::test]
async fn zstd_compression_round_trips_in_both_directions() {
    let client = start_server().await;
    let mut client = client
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd);

    let mut stream = client
        .extract(tokio_stream::iter(vec![
            pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Options(pb::ExtractOptions {
                    rules: vec![rule("p", "p")],
                    ..Default::default()
                })),
            },
            pb::ExtractRequest {
                frame: Some(pb::extract_request::Frame::Chunk(
                    b"<p>compressed</p>".to_vec(),
                )),
            },
        ]))
        .await
        .expect("a compressed call should open")
        .into_inner();

    let mut saw_finished = false;
    while let Some(event) = stream.message().await.unwrap() {
        if matches!(event.event, Some(Event::Finished(_))) {
            saw_finished = true;
        }
    }
    assert!(
        saw_finished,
        "the compressed stream should close with `finished`"
    );
}

// ---------------------------------------------------------------------------
// Stream lifecycle limits
// ---------------------------------------------------------------------------

/// A client that sends its options and then goes quiet loses the stream.
///
/// The idle timeout bounds how long a stalled upload can pin a parser. It is
/// idle, not total: a document of any length that keeps sending never trips
/// it, so the test stalls *between* frames and expects the call to end with
/// `DEADLINE_EXCEEDED` rather than an in-band error — the contract's error
/// taxonomy mirrors lol-html's, and a silent client is not a parse failure.
#[tokio::test]
async fn a_stalled_upload_is_ended_with_deadline_exceeded() {
    let mut client = start_configured_server(
        LolHtmlGrpc::new()
            .with_idle_timeout(std::time::Duration::from_millis(200))
            .into_service(),
    )
    .await;

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Options(pb::ExtractOptions {
            rules: vec![rule("p", "p")],
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Chunk(b"<p>hi</p>".to_vec())),
    })
    .await
    .unwrap();

    let mut stream = client
        .extract(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("open the call")
        .into_inner();

    // The first chunk's events arrive normally; then the client says nothing
    // and the server has to be the one to end it. `tx` is held open for the
    // whole loop so the silence is the client's choice, not a half-close.
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
    drop(tx);

    assert_eq!(
        error.map(|status| status.code()),
        Some(Code::DeadlineExceeded),
        "an idle client should be cut loose with DEADLINE_EXCEEDED"
    );
}

/// Past the concurrent-stream cap, a call is refused before a byte is read.
#[tokio::test]
async fn streams_past_the_concurrency_cap_fail_fast() {
    let mut client = start_configured_server(
        LolHtmlGrpc::new()
            .with_max_concurrent_streams(1)
            .into_service(),
    )
    .await;

    // The first call opens and stays open, holding the one permit.
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(pb::ExtractRequest {
        frame: Some(pb::extract_request::Frame::Options(everything())),
    })
    .await
    .unwrap();
    let mut first = client
        .extract(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .expect("the first call opens")
        .into_inner();
    assert!(matches!(
        first.message().await.unwrap().unwrap().event,
        Some(Event::Started(_))
    ));

    // The second is refused as a status on the call, before its options are
    // even read.
    let err = client
        .extract(tokio_stream::iter(vec![pb::ExtractRequest {
            frame: Some(pb::extract_request::Frame::Options(everything())),
        }]))
        .await
        .expect_err("a call past the cap should be refused");
    assert_eq!(err.code(), Code::ResourceExhausted);

    // Once the first stream is gone the permit comes back, so a retry works.
    // The release happens in the driver task, hence the brief grace period.
    drop(first);
    drop(tx);
    let mut reopened = None;
    for _ in 0..50 {
        match client
            .extract(tokio_stream::iter(vec![
                pb::ExtractRequest {
                    frame: Some(pb::extract_request::Frame::Options(everything())),
                },
                pb::ExtractRequest {
                    frame: Some(pb::extract_request::Frame::Chunk(b"<p>back</p>".to_vec())),
                },
            ]))
            .await
        {
            Ok(response) => {
                reopened = Some(response);
                break;
            }
            Err(status) => assert_eq!(status.code(), Code::ResourceExhausted),
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        reopened.is_some(),
        "the permit should be released when the first stream ends"
    );
}

// ---------------------------------------------------------------------------
// What one call may cost
// ---------------------------------------------------------------------------

/// The shape of a rule set is capped, and `ValidateSelectors` applies the same
/// caps as `Extract`, so a set it passes is never refused for its size.
#[tokio::test]
async fn a_rule_set_over_the_shape_limits_is_refused_by_both_rpcs() {
    use grpc_lol_html::rules::{MAX_RULES, MAX_SELECTOR_BYTES};

    let client = start_server().await;
    let too_many: Vec<_> = (0..=MAX_RULES)
        .map(|n| rule(&format!("r{n}"), "p"))
        .collect();
    let too_long = vec![rule(
        "long",
        &format!("div{}", ".c".repeat(MAX_SELECTOR_BYTES)),
    )];

    for rules in [too_many, too_long] {
        let err = extract(
            &client,
            b"<p>x</p>",
            pb::ExtractOptions {
                rules: rules.clone(),
                ..Default::default()
            },
            64,
        )
        .await
        .expect_err("Extract should refuse the rule set");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

        let err = client
            .clone()
            .validate_selectors(pb::ValidateSelectorsRequest { rules })
            .await
            .expect_err("ValidateSelectors should refuse it too");
        assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    }

    let at_the_cap: Vec<_> = (0..MAX_RULES)
        .map(|n| rule(&format!("r{n}"), "p"))
        .collect();
    let events = extract(
        &client,
        b"<p>x</p>",
        pb::ExtractOptions {
            rules: at_the_cap,
            ..Default::default()
        },
        64,
    )
    .await
    .expect("a rule set at the cap is fine");
    assert_eq!(
        only!(events, Event::Started)[0].rule_count as usize,
        MAX_RULES
    );
}

/// The caller picks its memory limit, but the server caps it: asking for
/// `u64::MAX` must not switch the parser's only memory guard off.
#[tokio::test]
async fn a_memory_limit_above_the_server_ceiling_is_cut_to_it() {
    let greedy = pb::ExtractOptions {
        limits: Some(pb::MemoryLimits {
            max_bytes: u64::MAX,
            ..Default::default()
        }),
        ..everything()
    };

    let capped = start_configured_server(
        LolHtmlGrpc::new()
            .with_max_memory_bytes(4096)
            .into_service(),
    )
    .await;
    let events = extract_file(&capped, "deep_nesting.html", greedy.clone(), 1024).await;
    let errors = only!(events, Event::Error);
    assert_eq!(
        errors.len(),
        1,
        "the 4 KiB ceiling should have stopped the parse"
    );
    assert_eq!(
        errors[0].code,
        pb::ParseErrorCode::MemoryLimitExceeded as i32,
        "{}",
        errors[0].message
    );

    // The same request against the default ceiling gets through, so it was
    // the ceiling, not the request, that stopped it above.
    let roomy = start_server().await;
    let events = extract_file(&roomy, "deep_nesting.html", greedy, 1024).await;
    assert!(matches!(
        events.last().and_then(|e| e.event.as_ref()),
        Some(Event::Finished(_))
    ));
}

/// A preallocation over the memory limit, including lol-html's own 1 KiB
/// default under a limit below that, is clamped rather than handed to
/// lol-html, which skips it silently in a release build and panics in a
/// debug one, taking the call down without a result.
#[tokio::test]
async fn a_preallocation_over_the_memory_limit_does_not_break_the_call() {
    let client = start_server().await;
    for limits in [
        pb::MemoryLimits {
            max_bytes: 4096,
            preallocated_buffer_bytes: 1 << 30,
            graceful_bail_out: false,
        },
        pb::MemoryLimits {
            max_bytes: 512,
            ..Default::default()
        },
    ] {
        let events = extract_file(
            &client,
            "doctype_legacy.html",
            pb::ExtractOptions {
                limits: Some(limits),
                ..everything()
            },
            64,
        )
        .await;
        assert!(
            matches!(
                events.last().and_then(|e| e.event.as_ref()),
                Some(Event::Finished(_) | Event::Error(_))
            ),
            "the call must end with a result, not a dropped stream ({limits:?}): {:?}",
            events.last()
        );
    }
}
