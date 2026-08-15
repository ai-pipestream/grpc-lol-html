# Benchmark harness

What does the gRPC layer cost, and what does a DOM cost?

```bash
cargo run --release                                  # synthetic 16 MiB, 5 iterations
cargo run --release -- --file page.html              # a real page
cargo run --release -- --mib 64 --iterations 7
cargo run --release -- --selectors "a[href],h2"      # vary the match count
cargo run --release -- --chunk 1048576               # vary the upload chunk
LOL_HTML_ADDR=10.0.0.5:50051 cargo run --release     # measure across a network
```

Its own crate with a path dependency back on the server, so it provably links
the same `lol_html` build, the same rule compilation and the same conversion
code as the thing it measures. It is excluded from the workspace and never
runs in CI.

## Three arms

| # | Arm | What it is |
|---|---|---|
| 0 | in-process lol-html | the ceiling. No serialization, no socket |
| 1 | over gRPC | what this repo ships, through a real socket |
| 2 | scraper | html5ever plus a selector engine, building a whole DOM |

Arms are **interleaved** rather than run in blocks, so thermal or scheduler
drift partway through hits all three rather than whichever ran last.

## The same-work gate

Numbers are refused unless the arms did the same work. Every matched element
folds into an order-sensitive FNV-1a digest of `(rule, tag, attributes)`, and
arms 0 and 1 must be **byte-identical**: the wire is supposed to carry exactly
what the parser found, so anything else is a bug rather than a benchmark.

```
0 in-process lol-html        1083ffcb35a0a38e/135426el/361136attr
1 over gRPC                  1083ffcb35a0a38e/135426el/361136attr
2 scraper (DOM)              71eb51f2077de438/135426el/361136attr
```

Arm 2 cannot join that gate by construction. A DOM query returns document order
per selector, not one interleaved stream, so its digest legitimately differs.
It is held to the weaker invariant that matters for a comparison: it must have
matched the same elements with the same attribute values. Each arm is also
checked for determinism across iterations.

## Reading the result

The synthetic corpus is deterministic and shaped like something worth scraping:
nested layout wrappers, links with several attributes, headings, prose of
varying length, images, and the `<script>` and `<style>` blocks that make up
much of a real page. A document that is 90% text and one that is
90% tags exercise completely different parts of a tokenizer, so the mix is not
incidental.

Use `--file` for a real page when the answer matters. The synthetic corpus is
for a repeatable number, not a representative one.

## Two things this harness got wrong first

Recorded because both are easy to repeat and neither is visible in the output.

**It configured its own subject differently from production.** The in-process
server was started with `Server::builder()` and nothing else, so it ran with
hyper's 1 MiB HTTP/2 window while `src/main.rs` ships 4 MiB. A benchmark that
misconfigures the thing it measures is measuring a program nobody runs. It now
mirrors the real transport settings, and `--window` makes the effect
measurable. On loopback it turned out to be worth almost nothing, which is
itself worth knowing.

**It found a real limit bug.** Uploading a 4 MiB chunk failed with
`OutOfRange: decoded message length too large`, because the server documented
an 8 MiB chunk cap while tonic's default decoding limit stayed at 4 MiB. Every
chunk between the two was refused by the transport, with a message about
decoded lengths rather than the `INVALID_ARGUMENT` that names the limit and
says to split the document. The server now derives tonic's limit from its own
cap at twice the value, and `an_oversized_chunk_is_refused_by_the_server_not_the_transport`
pins it.

## Caveats

Run-to-run variance on a busy machine is tens of percent, so the difference
between 95 and 123 MiB/s across two runs of the same configuration is noise,
not signal. Use `--iterations 7` or more before believing a comparison, and
re-run the harness rather than quoting [RESULTS.md](RESULTS.md) at different
hardware.

The gRPC arm runs client and in-process server on the same machine and the same
tokio runtime, so they compete for cores. That is the honest shape for "what
does gRPC cost", and the wrong shape for "what does the network cost", which is
what `LOL_HTML_ADDR` is for.
