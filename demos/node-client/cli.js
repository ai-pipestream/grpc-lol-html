// SPDX-License-Identifier: Apache-2.0
//
// Stream a local HTML file through grpc-lol-html and print one line per event.
//
//   node cli.js ../sample-data/cdata_svg.html "a[href]"
//   node cli.js page.html "title" "h1, h2" --spans --script-text
//
// With no selectors it uses a small default rule set: the title, every link,
// and the top three heading levels.

import fs from "node:fs";
import { LolHtmlClient, formatEvent } from "./lib/lolhtml.js";

const argv = process.argv.slice(2);
const flags = new Set(argv.filter((a) => a.startsWith("--")));
const positional = argv.filter((a) => !a.startsWith("--"));
const [file, ...selectors] = positional;

if (!file) {
  console.error("usage: node cli.js <file.html> [selector...] [--spans] [--script-text] [--all-text] [--raw-text] [--allow-ambiguous] [--encoding=LABEL]");
  process.exit(2);
}

const CAPTURES = ["CAPTURE_TAG_NAME", "CAPTURE_ATTRIBUTES", "CAPTURE_TEXT", "CAPTURE_COMMENTS", "CAPTURE_END_TAG"];
if (flags.has("--spans")) CAPTURES.push("CAPTURE_SOURCE_LOCATION");

const rules = selectors.length > 0
  ? selectors.map((selector, i) => ({ id: `r${i}`, selector, captures: CAPTURES }))
  : [
      { id: "title", selector: "title", captures: CAPTURES },
      { id: "links", selector: "a[href]", captures: CAPTURES },
      { id: "headings", selector: "h1, h2, h3", captures: CAPTURES },
    ];

const encodingFlag = argv.find((a) => a.startsWith("--encoding="));

const options = {
  rules,
  documentRule: { id: "doc", doctype: true, comments: true, text: false },
  encoding: encodingFlag ? encodingFlag.slice("--encoding=".length) : "",
  adjustCharsetOnMetaTag: true,
  allowAmbiguousMarkup: flags.has("--allow-ambiguous"),
  rawTextChunks: flags.has("--raw-text"),
  textTypes: textTypes(),
};

// Empty means the server's default of prose plus titles. `--script-text` adds
// the two that make up most of a real page's bytes; `--all-text` adds the last
// two as well, which only turn up in `<plaintext>` and in CDATA sections
// inside foreign content.
function textTypes() {
  if (flags.has("--all-text")) {
    return ["TEXT_TYPE_DATA", "TEXT_TYPE_RCDATA", "TEXT_TYPE_RAW_TEXT",
            "TEXT_TYPE_SCRIPT_DATA", "TEXT_TYPE_PLAIN_TEXT", "TEXT_TYPE_CDATA_SECTION"];
  }
  if (flags.has("--script-text")) {
    return ["TEXT_TYPE_DATA", "TEXT_TYPE_RCDATA", "TEXT_TYPE_SCRIPT_DATA", "TEXT_TYPE_RAW_TEXT"];
  }
  return [];
}

const client = new LolHtmlClient();

// Selector mistakes are the most common way to get an empty result, and
// checking costs one cheap round trip against no upload at all.
const { diagnostics } = await client.validateSelectors(rules);
if (diagnostics.length > 0) {
  for (const d of diagnostics) {
    console.error(`bad selector in rule ${d.ruleId}: ${d.selector}\n  ${d.code}: ${d.message}`);
  }
  client.close();
  process.exit(1);
}

try {
  for await (const event of client.extract(fs.readFileSync(file), options)) {
    const line = formatEvent(event);
    if (line !== null) console.log(line);
  }
} catch (err) {
  console.error(`rpc failed: ${err.message}`);
  process.exitCode = 1;
} finally {
  client.close();
}
