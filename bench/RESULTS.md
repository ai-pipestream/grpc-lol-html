# Captured results

Captured 2026-07-27. These describe one machine on one day. Re-run the harness
(see [README.md](README.md)) rather than quoting them against different
hardware.

Host: 32 logical cores, Linux 7.0.0-28-generic. Build: release profile, cargo
defaults, no `[profile.release]` overrides. Client and server share the machine
and the tokio runtime, so this measures what gRPC costs, not what a network
costs.

## The headline

**Over the wire, this service is faster than building a DOM in the same
process.**

```
  arm                                best        MiB/s  vs native
  in-process lol-html               49.3ms          324      1.00x
  over gRPC                        129.7ms          123      2.63x
  scraper (html5ever + DOM)        168.1ms           95      3.41x
```

Synthetic 16 MiB corpus, 256 KiB chunks, 7 iterations interleaved, 135,426
elements matched by `a[href], h2, p.body, img[src]`.

You pay about 2.6x against in-process lol-html for serialization and a socket,
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
  in-process lol-html              195.3ms          328      1.00x
  over gRPC                        520.8ms          123      2.67x
  scraper (html5ever + DOM)        674.8ms           95      3.46x
```

Throughput and ratios are unchanged from 16 MiB, which is what the streaming
design predicts: nothing here is proportional to document size except the
document.

## Upload chunk size is the knob that matters

Same 16 MiB document, varying only how many messages it becomes:

| chunk | messages | over gRPC |
|---|---|---|
| 16 KiB | 1024 | ~85 MiB/s |
| 64 KiB | 256 | ~82 MiB/s |
| 256 KiB | 64 | ~95 to 123 MiB/s |
| 1 MiB | 16 | ~96 MiB/s |
| 16 MiB | 1 | ~96 MiB/s |

It plateaus by 256 KiB and buys nothing above 1 MiB. The spread within the last
three rows is run-to-run variance, not a trend; do not read a ranking into it.
**256 KiB is the recommendation** and is the harness default.

## What the cost is not

Two hypotheses tested and rejected, so nobody re-tests them:

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

Run-to-run spread on a busy machine is tens of percent. The 256 KiB row above
measured 95 MiB/s in one sweep and 123 MiB/s in another, same configuration.
Use `--iterations 7` or more before believing any comparison, and prefer
re-running both arms to comparing against a number written here.
