# grpc-lol-html

A gRPC server that streams CSS-selector matches out of HTML, parsed with
[lol-html](https://github.com/cloudflare/lol-html), Cloudflare's streaming
rewriter and the engine behind Workers' `HTMLRewriter`.

```bash
cargo run --release
# INFO grpc_lol_html: grpc-lol-html listening addr=0.0.0.0:50057 window=4194304
```

## What it is

You send HTML chunks. It sends back what matched, as it matches.

```mermaid
sequenceDiagram
    participant C as Client
    participant S as LolHtmlService
    participant P as lol-html parser
    C->>S: options (rules, encoding, limits)
    S->>S: compile and validate rules
    S-->>C: started { encoding, rule_count }
    loop until half-close
        C->>S: chunk of document bytes
        S->>P: feed bytes
        P-->>S: element / text / comment / doctype
        S-->>C: match events, as they occur
    end
    C->>S: half-close
    S-->>C: finished { bytes_parsed, matches_by_rule } or terminal error
```

```
client ->  [0] options { rules: [{ id: "links", selector: "a[href]", captures: [ATTRIBUTES] }] }
client ->  [1] chunk (64 KiB)
server <-        started   { encoding: "UTF-8", rule_count: 1 }
server <-        element   { rule_id: "links", attributes: [href="/about"] }
client ->  [2] chunk
server <-        element   { rule_id: "links", attributes: [href="/contact"] }
client ->  half-close
server <-        finished  { bytes_parsed: 131072, matches_by_rule: { links: 2 } }
```

There is no `document_id` and no handle to close, and that is deliberate rather
than unfinished. The sibling [grpc-calamine](../grpc-calamine) service is
handle-based because a workbook is a random-access artifact: you upload it once
and read sheet 3, then sheet 1. lol-html is a forward-only transducer. Bytes go
in, events come out, nothing is retained, and there is no document to go back
to. Wrapping it in a handle store would mean holding the whole document
server-side and re-parsing per read, which throws away the one property the
library exists for.

So server memory does not grow with document size, and **the first matches
arrive before the last byte has been uploaded**. Both are tests rather than
claims. `matches_arrive_before_the_upload_is_finished` in `tests/extract.rs`
holds the second half of a document back and still demands its match, and
`tests/memory.rs` watches the real server binary's peak RSS while feeding it
successively larger documents:

```
  document      1 MiB    16 MiB    64 MiB   128 MiB   256 MiB
  peak RSS     30 MiB    42 MiB    42 MiB    42 MiB    43 MiB
```

256 times the document for less than one and a half times the memory, and
nearly all of that is the first step, where a freshly started process touches
its buffers for the first time. Retaining documents would have put the last
column near 300 MiB. (A release build, measured with the outbound buffer
described below in place; the same harness on the previous design, on the
same machine, peaked at 46 MiB.)

Not growing with the document is not the same as costing nothing. What one
call can hold is bounded by limits rather than by its input, and the bounds
are the ones under [Safety](#safety): parser state and reassembled text by the
call's memory limit, unsent events by the outbound buffer, one in-flight chunk
by the chunk cap, and the work per element by the cap on rules. A document of
any length, including one built to be hostile, stays inside them; the same
test feeds the server a single 64 MiB text node and watches the peak not move.

You can also watch it happen. `demos/node-client` has a web viewer that POSTs a
document and reads the events off the same response, drawing matches as they
land and marking the byte where the first one arrived:

```bash
cargo run --release &
cd demos/node-client && npm install && npm start   # http://127.0.0.1:8080
```

> First match after **48 B of 305 B** (16% uploaded). The rest of the document
> had not been sent yet.

## Configuration

All optional, read at startup. Every one but the address is a positive whole
number; zero or anything that does not parse stops the server with an error
naming the variable, rather than falling back to the default:

| Variable | Default | Effect |
|---|---|---|
| `GRPC_LOL_HTML_ADDR` | `0.0.0.0:50057` | listen address |
| `GRPC_LOL_HTML_WORKERS` | CPU count | tokio worker threads |
| `GRPC_LOL_HTML_MAX_CHUNK_BYTES` | 100 MiB | largest inbound chunk accepted |
| `GRPC_LOL_HTML_WINDOW_BYTES` | 4 MiB | HTTP/2 initial stream and connection window |
| `GRPC_LOL_HTML_IDLE_TIMEOUT_MS` | 60000 | end an `Extract` stream whose client stops sending |
| `GRPC_LOL_HTML_SEND_TIMEOUT_MS` | 60000 | end an `Extract` stream whose client stops reading |
| `GRPC_LOL_HTML_OUTBOUND_BUFFER_BYTES` | 8 MiB | unsent events one stream may hold before the parse waits for its client |
| `GRPC_LOL_HTML_MAX_MEMORY_BYTES` | 64 MiB | ceiling on the memory limit a call may ask for |
| `GRPC_LOL_HTML_MAX_CONCURRENT_STREAMS` | 64 | cap on open `Extract` streams; past it, calls fail with `RESOURCE_EXHAUSTED` |

The idle timeout is idle, not total: it resets with every frame, so a document
of any length that keeps sending never trips it. A stalled upload gets
`DEADLINE_EXCEEDED`, not an in-band `error` event — the contract's error
taxonomy mirrors lol-html's, and a silent client is not a parse failure. The
stream cap is per-process and per-parser, orthogonal to tonic's per-connection
`max_concurrent_streams` (1024): each open stream is a parser instance with
its own buffers, so an unbounded count would be an unbounded memory
commitment.

The send timeout is the same idea in the other direction, and idle in the same
way: every response the client takes starts it over, so a slow reader never
reaches it. A response is taken as soon as the transport has room for it, and
a text node of megabytes can take longer than the timeout to read, so the
timeout starts over only once the client could have read what it took at
64 KiB/s. A client reading at least that fast is never cut off mid-response;
one that stops reading keeps its slot past the timeout for as long as its
largest recent response takes at that rate: 16 seconds a MiB.

Events wait in a per-stream buffer bounded in bytes; when it is full the parse
waits for the client, and so does the reading of the upload. A client that takes nothing for the send timeout gets `RESOURCE_EXHAUSTED`,
its unsent events are discarded, and its stream slot goes back to the pool,
so 64 clients that never read cannot lock the service out. Set the buffer
below 64 KiB and events go out in DATA frames small enough for some HTTP/2
clients to treat as a flood.

Logs go through [tracing](https://docs.rs/tracing): `RUST_LOG` picks the
filter (default `info`), and at that level each finished stream logs one line
with its bytes parsed, matches, bail-out flag and duration.

The server also answers the standard gRPC health protocol
(`grpc.health.v1.Health/Check`): both the empty service name and
`lolhtml.v1.LolHtmlService` report `SERVING`, so an orchestrator can probe
the service rather than the port.

## Where you split the upload is invisible

Chunk size is a throughput knob and nothing else. `tests/extract.rs` replays
every fixture at 1, 3, 7, 64, 1024 and whole-document chunk sizes and compares
the resulting event streams **in full**: every field, including byte spans and
the final `bytes_parsed`.

This is the load-bearing test, and it earned its keep: it is what caught both
of the upstream problems described under "Two things lol-html gets wrong",
below. Neither was visible from reading the library's documentation.

## The six things clients get wrong

### 1. Text chunks are not text nodes

lol-html splits text at buffer boundaries, so one paragraph can arrive as any
number of fragments with `last_in_text_node()` marking the real end. Consuming
those directly gives you a client that looks perfect in testing and cuts words
in half in production, at document sizes you did not test.

The server reassembles by default and emits one `text` event per text node.
Set `raw_text_chunks` if you want the fragments, which keeps server memory
constant even for a single enormous text node; concatenating a node's fragments
in order reproduces the reassembled text exactly. Reassembly holds the node
until it ends, so that text counts against the call's memory limit, and a node
larger than the limit ends the run with `MEMORY_LIMIT_EXCEEDED` (or a
`bailed_out` finish, under `graceful_bail_out`) rather than arriving in
pieces nobody asked for.

### 2. It does not run JavaScript

No engine, no DOM, no fetching of `<script src>`. This is a tokenizer, and
whatever the server sent is what you get.

It does extract script *source*, which is the useful half: structured data
lives in `<script type="application/ld+json">` on most commercial pages, and a
selector reaches it. Price, availability and rating without a browser.

The limit that follows is real. On a client-rendered app the content is not in
the HTML at all: `sample-data/spa_shell.html` yields the string `Loading…` and
nothing else, because the `<div id="root">` is filled by a bundle this service
will never run. If you need the post-JavaScript DOM, render it with a headless
browser first and feed the result here.

### 3. Not all text is prose

`TextType` distinguishes six kinds of text, and a client that concatenates
every text event indexes minified JavaScript and CSS as body copy.

The default filter is `[DATA, RCDATA]`: prose, plus the `<title>` and
`<textarea>` contents that are also entity-decoded. Ask for `SCRIPT_DATA` or
`RAW_TEXT` by name if you want them. Only `DATA` and `RCDATA` are
entity-decoded, so the rest arrive exactly as written.

### 4. Ambiguous markup is refused, not guessed

On markup like `<select><xmp><script>` lol-html declines to pick a parse,
because picking wrong is what turned Cloudflare's own security features into
[XSS gadgets](https://portswigger.net/blog/when-security-features-collide). It
arrives as a typed terminal `error` event; everything emitted before it stands.

The escape hatch is `allow_ambiguous_markup`, deliberately phrased as an opt-in
to the unsafe behaviour rather than as lol-html's own `strict` flag. proto3 has
no field presence for bools, so a `strict` field would arrive as `false` from
every client that had not heard of it, and the safe behaviour has to be the
zero value. Conforming markup never triggers the bail-out.

### 5. Send the options frame before you await the response

The server validates options before opening the response stream, so it emits no
response headers until it has them. A client that feeds the request from a
queue and awaits the call *first* will wait forever. Clients that build the
request as an iterator never notice.

### 6. Read while you upload, not after

The server holds a bounded amount of output for each stream. When the client
is not taking it, the parse waits, and so does the reading of the upload,
which is how backpressure works and why a slow client costs the server
nothing. A client that writes the whole document before its first read turns
that into a standoff once the output outgrows the buffer: its upload blocks on
a full HTTP/2 window and it never gets to the read that would unblock it.
After the send timeout the server gives up, keeps reading the upload so the
client can finish it, and answers the first read with `RESOURCE_EXHAUSTED`.
Small outputs fit in the buffer and work anyway, which is what makes this easy
to ship. Read on a second thread, or with an async or callback API, the same
as any bidirectional stream.

## Selectors: what compiles and what does not

Wider than "no tree, no selectors" suggests, and the line is worth knowing.

**Supported.** `*`, `E`, `E F`, `E > F`, `.class`, `#id`, `:not(s)`, every
attribute operator including `^=` `$=` `*=` `~=` `|=` and the `i`/`s` case
flags, plus `:nth-child(n)`, `:first-child`, `:nth-of-type(n)` and
`:first-of-type`.

**Rejected, because the answer lies in markup not yet read.** `:last-child`,
`:only-child`, `:nth-last-child(n)`, `:last-of-type`, `:only-of-type`,
`:has(s)`.

**Rejected as unimplemented.** The sibling combinators `+` and `~`.

**Rejected as meaningless in a token stream.** Namespaced selectors (`svg|a`)
and pseudo-elements (`::before`).

`:first-child` against `:last-child` is the whole idea: an element is known to
be first the moment it is seen, and cannot be known to be last until its parent
closes. None of these are pending work.

`ValidateSelectors` compiles a rule set and returns typed diagnostics without
sending a document. One cheap round trip, and selector mistakes are the most
common way to get an empty result out of this service.

`GetServiceInfo` reports the server's name, build version and the shared
`UiInfo` advertisement the ai-pipestream demo shell reads to mount this
service's web frontend as a tab.

## Safety

**Memory is capped whether or not you ask.** lol-html defaults
`max_allowed_memory_usage` to `usize::MAX`; a server taking documents off the
open web must not run that way, so an unset or zero `limits.max_bytes` means
64 MiB rather than infinity. Exceeding it is a typed error, or a truncated
success carrying `bailed_out` if you set `graceful_bail_out`. Either way the
process survives, which
`a_document_over_its_memory_cap_fails_in_band_and_the_server_survives` checks
by streaming a second document through afterwards.

**And the caller does not get the last word on it.** A request can lower its
limit but not raise it past the server's ceiling, `GRPC_LOL_HTML_MAX_MEMORY_BYTES`
(64 MiB by default); asking for more gets the ceiling, so no caller can switch
the parser's memory guard off by asking for `u64::MAX`. The text the server
holds while reassembling text nodes, summed across rules, is something
lol-html's own accounting never sees, so it has a budget of its own of the
same size: a call can hold up to twice its limit, the parser's state and the
text beside it. A preallocation larger than the limit is cut to it.

**Every buffer a call can fill has a bound.** Parser state, and separately
reassembled text: the memory limit each. Unsent events: the outbound buffer,
8 MiB per stream, with the parse waiting on the client beyond that, and the
call ended after the send timeout if the client takes nothing. One event
larger than the whole buffer still goes out, alone, so a single text node can
take the buffer past its bound up to the memory limit. One inbound chunk: the
chunk cap. The work and the output per element: at most 256 rules, with ids
of at most 256 bytes and selectors of at most 4096. So a stream's memory has a
ceiling set by configuration, and the process's by that times the stream cap;
nothing in between depends on what the document says or how long it is.

**UTF-16 is refused up front.** lol-html's tokenizer scans for ASCII markup
bytes, so UTF-16LE/BE, ISO-2022-JP and `replacement` are rejected with
`INVALID_ARGUMENT`. Transcode first.

**Chunk size is capped** at 100 MiB, tunable with
`GRPC_LOL_HTML_MAX_CHUNK_BYTES`. This is not a document size limit; a document
is any number of chunks and has no ceiling. It bounds how much one message
carries, and so how long a single uninterruptible parse runs and how large a
buffer each in-flight call holds. tonic's own decoding limit is derived from it
at twice the value, deliberately above rather than equal, so an ordinary
overshoot gets the `INVALID_ARGUMENT` that names the limit rather than the
transport's `OutOfRange`.

**Parsing never runs on an async worker.** Every chunk is parsed on tokio's
blocking pool, so even a 100 MiB one is one long parse on a thread nobody else
is waiting for, rather than seconds in which health checks, keepalives and
other streams cannot be served.
`a_large_chunk_is_parsed_without_holding_up_other_calls` holds a call open
mid-parse on a single-threaded runtime and requires it to be answered.

Sending a chunk that large is allowed but pointless. The benchmark plateaus
around 256 KiB: 16 KiB against 1 MiB is roughly 85 against 96 MiB/s, and above
that nothing improves while the per-call buffer grows.

## Two things lol-html gets wrong

Both found by the chunk-size invariance test, both worked around at the
boundary rather than passed on to clients.

**Bare attribute source locations are unusable.** For an attribute written
without a value, lol-html reports a position that varies with chunking and can
point at bytes belonging to a different part of the tag. Reproduced directly
against the library on `<script SRC="/a.js" DEFER async>`:

```
chunk 8:   defer: name=Some(33..38)  value=Some(13..13)
chunk 64:  defer: name=None          value=None
```

Byte 13 is the start of `<script`. A real value always begins after its own
name ends, so the server drops any name/value span pair where it does not, and
bare attributes therefore carry no spans at all. `foo=""` is a real location
and keeps its spans.

**`Doctype::force_quirks()` is not public API.** lol-html computes it but gates
the accessor behind its internal `_integration_test` feature. The field number
is reserved in the proto rather than shipped always-false, so it can come back
unchanged if that accessor is ever exposed.

## Compression

The server speaks zstd in both directions — it decompresses requests and
compresses responses — but a response is compressed only for a client that
advertised the encoding, so nothing changes for one that did not ask. Match
events are repetitive small messages and compress well.

With tonic, opt in on the client:

```rust
let mut client = LolHtmlServiceClient::new(channel)
    .accept_compressed(CompressionEncoding::Zstd) // compressed responses
    .send_compressed(CompressionEncoding::Zstd); // compressed uploads
```

(grpcurl's `-encoding` knows gzip only; use a real client for zstd.)

## Is it fast

Faster over the wire than building a DOM in the same process:

```
  arm                                best        MiB/s  vs native
  in-process lol-html               41.2ms          389      1.00x
  over gRPC                         89.8ms          178      2.18x
  scraper (html5ever + DOM)        144.3ms          111      3.50x
```

16 MiB synthetic page, 135,426 matched elements, 7 interleaved iterations on a
32 core host. You pay about 2.2x against in-process lol-html for serialization
and a socket, and still beat html5ever with a DOM, which pays no transport cost
at all. Ratios are unchanged at 64 MiB.

`bench/` refuses to print numbers unless the arms agree: every matched element
folds into an order-sensitive digest, and in-process and over-the-wire must be
byte-identical. Chunk size matters less than it used to: everything from
16 KiB to 1 MiB lands within run-to-run variance, and clients that can buffer
the document whole measure fastest of all. See
[bench/RESULTS.md](bench/RESULTS.md), including two hypotheses about where the
cost goes that turned out to be wrong.

## Docker

```bash
docker build -t grpc-lol-html .
docker run --rm --read-only --cap-drop ALL --security-opt no-new-privileges \
  -p 50057:50057 grpc-lol-html
```

The build stage runs the test suite and then compiles with fat LTO and
`panic = "abort"`; a red suite fails the image rather than shipping. The
runtime is `dhi.io/debian-base:trixie-debian13` and runs as a non-root user
with no shell, which is why
health checking is the orchestrator's job over `grpc.health.v1.Health/Check`
rather than a Dockerfile `HEALTHCHECK`. Codegen is checked in, so the build
needs neither buf nor protoc.

## Building

```bash
cargo build --release
cargo test                                              # 84 tests
cargo clippy --all-targets --all-features -- -Dwarnings
buf lint && buf build
buf generate                                            # regenerate src/gen
buf build -o src/gen/file_descriptor_set.binpb          # regenerate the reflection descriptor set
demos/compare-clients.sh                                # needs a running server
```

The server also exposes gRPC reflection (v1) from a `FileDescriptorSet` checked
in at `src/gen/file_descriptor_set.binpb`: the same `buf build` output, kept
next to the generated Rust since codegen runs through buf rather than a
build.rs. Rebuild it after any proto change; with it, clients such as
`grpcurl -plaintext localhost:50057 list` need no local .proto files.

MSRV is 1.88, set by tonic 0.14 rather than by lol-html, which builds on 1.85.

The contract is the deliverable: see
[`proto/lolhtml/v1`](proto/lolhtml/v1), whose comments document every field.
`buf lint` enforces a comment on every message, enum, field, oneof and RPC.

## Not here yet

**Rewriting.** lol-html rewrites as well as it reads, and `Rewrite` will be a
second RPC on the same service carrying declarative mutations that map onto
lol-html's `Element` methods. Adding an RPC is additive, which is why the
service is named for the library rather than for `Extract`: a service name is
baked into every method path and is the one thing here that cannot be changed
additively later.

**An html5ever backend.** Deliberately not reserved as a `backend` field.
html5ever builds a tree, so it structurally cannot emit events before the
document ends, and a field implying a drop-in swap would quietly change the
streaming guarantee this contract is built on. If it lands it deserves its own
RPC that is honest about buffering.
