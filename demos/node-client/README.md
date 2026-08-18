# Node demo client

Two programs against the same contract: a CLI streamer and a live web viewer.
Stubs are loaded dynamically from [`../../proto`](../../proto) at run time, so
nothing generated is checked in.

```bash
npm install

# CLI
node cli.js ../sample-data/cdata_svg.html "a[href]"

# Web viewer, then open http://127.0.0.1:8080
npm start
```

Both honour `LOL_HTML_ADDR` (default `127.0.0.1:50053`). The viewer also takes
`PORT` (default 8080).

## The web viewer

The viewer exists to make one property visible: **matches arrive before the
upload finishes**.

It is a single HTTP request. The browser POSTs the document and reads
Server-Sent Events off the *same* response, which is deliberately the same
shape as the gRPC call underneath it: bytes going one way while events come
back the other. Nothing buffers the document. Each upload slice is written
into the gRPC call as it lands, and each event is flushed to the page as the
Rust server emits it.

The page shows an upload bar with a green marker where the first match landed,
and says so in words:

> First match after **48 B of 305 B** (16% uploaded). The rest of the document
> had not been sent yet.

Two controls make that observable rather than theoretical:

- **Upload throttle** sleeps between upload slices. It slows the *upload* only.
  The parser is never waiting on anything but bytes.
- **Chunk size** is derived from the document, aiming for about 40 upload steps
  whatever the size, and shown next to the throttle. A 300 byte fixture and a
  300 KiB page then look the same.

Neither changes the events. The Rust suite pins that by replaying every fixture
at six chunk sizes and comparing the streams in full.

Worth trying:

| Document | What you see |
|---|---|
| `cdata_svg.html` | two `<a>` elements distinguished only by namespace, one purple for SVG |
| `ambiguity_select_xmp_script.html` | matches delivered, then the server refusing to guess, in red |
| `script_and_style_text.html` | no JavaScript in the text feed, because it is not prose |
| `deep_nesting.html` | 22 KiB and 2000 levels deep, where the throttle stops mattering |

### A real page

The small fixtures each pin one hazard; none of them show what the service is
actually for. Drop any HTML worth megabytes into
[`../sample-data/large/`](../sample-data/large) and it appears in the dropdown,
or use the file picker for something on your disk. The one in the docs is the
WHATWG HTML spec:

```bash
curl -sL --compressed -o ../sample-data/large/html-spec.html https://html.spec.whatwg.org/
```

14.8 MiB, and with `a[href], h2, code, dfn` it produces **415,212 events and
107,228 matches** in about three seconds of browser time, first match at 29%
uploaded. The feed holds 600 rows however many events arrive.

The throughput the page reports is browser time: throttle, SSE bridge, JSON
parsing and rendering all included. It is roughly an order of magnitude below
what the service does on its own, which is what [`../../bench`](../../bench)
measures.

### Why SSE is parsed by hand

`EventSource` only does `GET`, and the whole point is that the upload and the
event stream are one request. So the page reads `response.body` as a stream and
splits frames itself. It is about fifteen lines and it is in `run()` in
`public/index.html`.

## Things that bite

**Write the options frame before you read the response.** The server validates
options before opening the response stream, so it emits no response headers
until it has them. A client that awaits the call first, then feeds the request
from a queue, deadlocks. `lib/lolhtml.js` sends options inside `openExtract()`
for exactly this reason, so the ordering cannot be got wrong by a caller.

**Handle the oneof by name, not by guessing.** With `oneofs: true`,
proto-loader sets `message.event` to the name of the active arm. The bridge
forwards `message[message.event]` rather than sniffing which key is populated,
so an arm added to the contract later is passed through to the page instead of
being dropped silently.

**Backpressure is real and worth keeping.** `res.write()` returning false means
the browser is behind. The bridge pauses the gRPC call and resumes on `drain`,
which propagates through gRPC flow control back to the parser. Without it, a
large page queues the whole event stream in this process.
