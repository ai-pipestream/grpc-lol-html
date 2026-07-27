// SPDX-License-Identifier: Apache-2.0

//! A synthetic page, so the benchmark runs without shipping a large fixture
//! or depending on a URL that will rot.
//!
//! Shaped to look like something worth scraping rather than to flatter any
//! particular parser: nested layout wrappers, links with several attributes,
//! headings, paragraphs of real-ish prose, images, and the `<script>` and
//! `<style>` blocks that make up a depressing share of a real page's bytes.
//! The mix matters, because a document that is 90% text and a document that
//! is 90% tags exercise completely different parts of a tokenizer.
//!
//! Deterministic: same size in, same bytes out, so two runs are comparable.

/// Build a page of roughly `mib` mebibytes.
pub fn synthetic(mib: usize) -> Vec<u8> {
    let target = mib * 1024 * 1024;
    let mut out = String::with_capacity(target + 4096);

    out.push_str(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <title>Synthetic benchmark corpus</title>\n\
         <style>body{font:16px/1.5 system-ui;margin:0}.card{padding:12px}</style>\n\
         <script>window.__bench = {started: 0, items: []};</script>\n\
         </head>\n<body>\n<main id=\"main\">\n",
    );

    let mut n = 0usize;
    while out.len() < target {
        section(&mut out, n);
        n += 1;
    }

    out.push_str("</main>\n</body>\n</html>\n");
    out.into_bytes()
}

/// One repeating block, about 1 KiB.
fn section(out: &mut String, n: usize) {
    // Pseudo-random but deterministic, so the shape varies without a dependency.
    let salt = n.wrapping_mul(2_654_435_761) % 997;

    out.push_str("<section class=\"card\" data-index=\"");
    out.push_str(&n.to_string());
    out.push_str("\">\n  <h2 id=\"h");
    out.push_str(&n.to_string());
    out.push_str("\">Section ");
    out.push_str(&n.to_string());
    out.push_str("</h2>\n  <div class=\"row\"><div class=\"col\">\n");

    for link in 0..3 {
        out.push_str("    <a href=\"/item/");
        out.push_str(&(salt + link).to_string());
        out.push_str("\" rel=\"noopener\" title=\"Item ");
        out.push_str(&(salt + link).to_string());
        out.push_str("\">Item ");
        out.push_str(&(salt + link).to_string());
        out.push_str("</a>\n");
    }

    out.push_str("    <p class=\"body\">");
    // Vary the prose length so text-node handling is not uniform.
    for word in 0..(12 + salt % 40) {
        out.push_str(PROSE[(salt + word) % PROSE.len()]);
        out.push(' ');
    }
    out.push_str("</p>\n");

    out.push_str("    <img src=\"/img/");
    out.push_str(&salt.to_string());
    out.push_str(".webp\" alt=\"Figure ");
    out.push_str(&salt.to_string());
    out.push_str("\" loading=\"lazy\" width=\"640\" height=\"360\">\n");

    // Script and style inline the way real pages do, which is also the text
    // the service refuses to call prose unless asked.
    if n.is_multiple_of(7) {
        out.push_str("    <script>__bench.items.push({id:");
        out.push_str(&salt.to_string());
        out.push_str(",ok:true});</script>\n");
    }

    out.push_str("  </div></div>\n</section>\n");
}

const PROSE: &[&str] = &[
    "streaming",
    "parser",
    "document",
    "throughput",
    "selector",
    "match",
    "element",
    "attribute",
    "buffer",
    "boundary",
    "chunk",
    "encoding",
    "namespace",
    "tokenizer",
    "handler",
    "backpressure",
    "memory",
    "limit",
    "ambiguous",
    "markup",
    "rewriter",
    "extraction",
    "pipeline",
    "ingestion",
];
