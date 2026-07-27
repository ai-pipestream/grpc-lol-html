# grpc-lol-html demos

**These are examples, not a client library.** Nothing here is packaged,
versioned, or published to npm, PyPI, or Maven Central, and nothing depends on
it. Each demo is a short readable file meant to be *copied from*: the
deliverable is the contract in [`../proto`](../proto), and these show what
talking to it looks like in three languages. Every one generates (or
dynamically loads) its stubs from the repository's proto files at build or run
time, so no generated client code is checked in.

Start the server first, from the repository root:

```bash
cargo run --release
# grpc-lol-html listening on 0.0.0.0:50051
```

| Demo | Stack | Run it |
|---|---|---|
| [`node-client`](node-client) | Node 20+, `@grpc/grpc-js` | CLI: `npm install && node cli.js ../sample-data/cdata_svg.html "a[href]"` <br> **Web viewer: `npm start`, then http://127.0.0.1:8080** |
| [`python-client`](python-client) | Python 3.11+, `grpcio` | `./run.sh ../sample-data/cdata_svg.html "a[href]"` |
| [`java-client`](java-client) | Java 17+, Maven, `grpc-java` | `mvn -q compile exec:java -Dexec.args="../sample-data/cdata_svg.html a[href]"` |

**Start with the web viewer.** It is the one that shows what this service is
for: matches drawn on the page while the upload bar is still filling, with a
marker at the byte where the first one landed. See
[`node-client/README.md`](node-client/README.md).

All three take the same arguments, honour `LOL_HTML_ADDR` (default
`127.0.0.1:50051`), and print **byte-identical output**. That agreement is the
point of having three of them: each reads the same contract through a different
generated-code toolchain, so agreement is evidence the contract says what it
means, and divergence is evidence one of them is guessing.

```bash
./compare-clients.sh            # every fixture through all three, diffed
./compare-clients.sh cdata_svg.html
```

## The output format

One line per event, terse and field-ordered so the three can be diffed:

```
$ node cli.js ../sample-data/cdata_svg.html "a[href]"
started encoding=UTF-8 rules=1
doctype rule=doc name="html" public="" system=""
element rule=r0 tag=a ns=NAMESPACE_SVG void=false attrs=[href="#target"]
endtag rule=r0 tag=a
element rule=r0 tag=a ns=NAMESPACE_HTML void=false attrs=[href="#html"]
text rule=r0 type=TEXT_TYPE_DATA last=true value="An HTML anchor, same tag name, different namespace."
endtag rule=r0 tag=a
finished bytes=305 bailed=false counts=[r0:2]
```

Two `<a>` elements with the same tag name in different namespaces, which is
the sort of thing that quietly poisons a link extractor built on tag names
alone.

Common flags: `--spans` (byte ranges), `--script-text` (also report `<script>`
and `<style>` contents), `--raw-text` (unreassembled text fragments),
`--allow-ambiguous`, `--encoding=LABEL`.

## Things that bite

**Send the options frame before you await the response.** The server validates
options before opening the response stream, so it emits no response headers
until it has them. A client that feeds the request from a queue and awaits the
call first will wait forever. All three demos write options first; the Node one
has the ordering called out in a comment because it is the one that looks most
reorderable.

**Regenerate Python stubs every run.** `python-client/gen/` is gitignored, and
a stale copy produces a client that silently drops any `oneof` arm added since
it was generated: `WhichOneof` returns `None` for an unknown variant and the
event disappears with no error anywhere. `run.sh` therefore regenerates
unconditionally rather than checking timestamps. This exact failure cost real
debugging time on the sibling grpc-calamine service.

**Check your selectors first.** All three call `ValidateSelectors` before
uploading anything. A selector a browser accepts may still be rejected here:
`:last-child` and the sibling combinators cannot be evaluated by a parser that
never builds a tree. One cheap round trip beats an empty result you have to
explain.

**Don't concatenate every text event.** Check `type`. The default filter
already excludes `<script>` and `<style>`, but if you pass `--script-text` you
get them and they are not prose.

## Sample data

[`sample-data/`](sample-data) holds small hand-written HTML files, each pinning
one thing the server has to get right. The Rust integration tests replay all of
them at six different chunk sizes and compare the streams in full.

| File | Pins |
|---|---|
| `ambiguity_select_xmp_script.html` | the strict-mode bail-out, as a typed in-band error |
| `text_split_boundary.html` | a text node long enough to straddle any small chunk size |
| `deep_nesting.html` | 2000 nested divs, enough to trip a low memory cap |
| `script_and_style_text.html` | every `TextType` in one document |
| `charset_meta_windows1251.html` | `<meta charset>` overriding the declared encoding |
| `utf16.html` | an encoding lol-html cannot tokenize at all |
| `cdata_svg.html` | SVG namespace, CDATA text, and an `<a>` in each namespace |
| `doctype_legacy.html` | a doctype carrying public and system identifiers |
| `unclosed_tags.html` | elements that never close, so no end-tag events |
| `duplicate_and_bare_attrs.html` | attribute case, bare attributes, and their spans |

These are all tiny, because each one exists to pin a single hazard. For
something that looks like real work, see
[`sample-data/large/`](sample-data/large): drop in a page worth megabytes and
it shows up in the viewer's dropdown and can be handed to the bench with
`--file`.

Try the ones that fail on purpose:

```bash
node cli.js ../sample-data/ambiguity_select_xmp_script.html '*'
# ...
# element rule=r0 tag=select ns=NAMESPACE_HTML void=false attrs=[]
# error code=PARSE_ERROR_CODE_PARSING_AMBIGUITY message="The parser has encountered a text content tag (`<xmp>`) ..."

node cli.js ../sample-data/utf16.html '*' --encoding=utf-16le
# rpc failed: encoding `UTF-16LE` is not ASCII-compatible and cannot be tokenized by lol-html: ...
```
