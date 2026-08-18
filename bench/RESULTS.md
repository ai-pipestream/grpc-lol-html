# Captured results

Captured 2026-08-18. These describe one machine on one day. Re-run the harness
(see [README.md](README.md)) rather than quoting them against different
hardware.

Host: 32 logical cores, Linux 7.0.0-28-generic. Build: the release profile the
repo now ships — fat LTO, one codegen unit, mimalloc. The previous capture
(2026-07-27) ran cargo defaults and read 324 native / 123 over gRPC / 95 DOM,
so the profile and allocator change alone moved the wire arm by roughly 45
percent on this machine. Client and server share the machine and the tokio
runtime, so this measures what gRPC costs, not what a network costs.

## The headline

**Over the wire, this service is faster than building a DOM in the same
process.**

```
  arm                                best        MiB/s  vs native
  in-process lol-html               41.2ms          389      1.00x
  over gRPC                         89.8ms          178      2.18x
  scraper (html5ever + DOM)        144.3ms          111      3.50x
```

Synthetic 16 MiB corpus, 256 KiB chunks, 7 iterations interleaved, 135,426
elements matched by `a[href], h2, p.body, img[src]`.

You pay about 2.2x against in-process lol-html for serialization and a socket,
and still come out ahead of html5ever with a DOM, which pays no transport cost
at all. That is the trade this service is making: a network hop and a language
boundary, for less than what a tree costs.

## Same-work proof

```
0 in-process lol-html        1083ffcb35a0a38e/135426el/361136attr
1 over gRPC                  1083ffcb35a0a38e/135426el/361136attr
2 scraper (DOM)              71eb51f2077de438/135426el/361136attr

ok   arms 0 and 1 are identical, so gRPC changed nothing but the transport
ok   arm 2 matched the same 135426 elements, grouped by selector rather than interleaved
```

## It scales linearly

Same configuration at 64 MiB, 540,900 matches:

```
  in-process lol-html              165.5ms          387      1.00x
  over gRPC                        359.1ms          178      2.17x
  scraper (html5ever + DOM)        858.1ms           75      5.19x
```

Throughput for the lol-html arms is unchanged from 16 MiB, which is what the
streaming design predicts: nothing here is proportional to document size except
the document. The DOM arm's number wobbles badly at this size — 75 MiB/s here,
25 in another run of the same configuration — as the retained tree stops
fitting in cache. It stays far from the wire arm in either reading, which is
the only claim this section makes about it.

## Upload chunk size matters less than it used to

Same 16 MiB document, varying only how many messages it becomes:

| chunk | messages | over gRPC |
|---|---|---|
| 16 KiB | 1025 | ~131 MiB/s |
| 64 KiB | 257 | ~120 MiB/s |
| 256 KiB | 65 | ~127 to 178 MiB/s |
| 1 MiB | 17 | ~121 MiB/s |
| 16 MiB | 2 | ~170 MiB/s |

The old capture showed a plateau by 256 KiB and nothing gained above 1 MiB;
that is no longer what the machine says. Rows from 16 KiB to 1 MiB are all
inside run-to-run variance of each other, and the two-message extreme measured
~170 MiB/s in both a 5-iteration and a 7-iteration run — repeatable, not noise.
Per-message cost got cheap enough (LTO, mimalloc) that the number of messages
mostly stops mattering, and a client that can buffer the document whole does
best of all. Chunk at whatever size the client already has; there is no longer
a sweet spot to hit.

## What the cost is not

Two hypotheses tested and rejected on the earlier capture, so nobody re-tests
them. The mechanism has not changed, only the constants:

**Not the response messages.** A selector matching nothing still costs 183 ms
against 15 ms native on the same 16 MiB document. With zero events returned,
the overhead is entirely in getting the bytes to the parser.

**Not HTTP/2 flow control.** Raising the window from hyper's 1 MiB default to
the 4 MiB the server ships moved the result from 195.5 ms to 193.1 ms, which is
noise. Worth fixing anyway, because the harness should configure its subject
the way production does, but it was not the answer.

The remaining cost is per-byte work in the transport: encode, frame, socket,
decode. It is proportional to the document and not to the match count.

## Variance

Run-to-run spread on a busy machine is tens of percent. Identical 256 KiB
configurations in this capture measured 127, 131 and 178 MiB/s on the wire arm.
Use `--iterations 7` or more before believing any comparison, and prefer
re-running both arms to comparing against a number written here.
