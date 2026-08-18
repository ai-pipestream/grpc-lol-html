// SPDX-License-Identifier: Apache-2.0
//
// Thin wrapper around the lolhtml.v1 gRPC contract.
//
// The protos are loaded dynamically from ../../proto (the single source of
// truth in this repository) — no generated code is checked in.

import { fileURLToPath } from "node:url";
import path from "node:path";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const PROTO_ROOT = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "..", "..", "..", "proto",
);

const packageDefinition = protoLoader.loadSync(
  path.join(PROTO_ROOT, "lolhtml", "v1", "lolhtml_service.proto"),
  {
    includeDirs: [PROTO_ROOT],
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  },
);

const { lolhtml } = grpc.loadPackageDefinition(packageDefinition);

/** Upload chunk size. Any value gives the same events; this one is quick. */
export const CHUNK_BYTES = 64 * 1024;

/** A connected grpc-lol-html client. */
export class LolHtmlClient {
  /** @param {string} address host:port of the grpc-lol-html server. */
  constructor(address = process.env.LOL_HTML_ADDR ?? "127.0.0.1:50053") {
    this.stub = new lolhtml.v1.LolHtmlService(
      address,
      grpc.credentials.createInsecure(),
    );
  }

  /**
   * Open an Extract call and send the options frame.
   *
   * The caller then writes `{ chunk }` frames as the document becomes
   * available and calls `.end()`. This is the shape to use when the document
   * is itself arriving from somewhere, since it never holds the whole thing:
   * `server.js` pipes an HTTP upload straight through it.
   *
   * Note the ordering: options go out before anything reads the response. The
   * server validates them before opening the response stream, so it produces
   * no response headers until it has them, and a caller that waits first
   * waits forever.
   *
   * @param {object} options an ExtractOptions message.
   * @returns {object} the duplex call.
   */
  openExtract(options) {
    const call = this.stub.extract();
    call.write({ options });
    return call;
  }

  /**
   * Stream a whole in-memory document and yield each event as it arrives.
   *
   * @param {Buffer} bytes the document.
   * @param {object} options an ExtractOptions message.
   * @returns {AsyncGenerator<object>} ExtractResponse messages.
   */
  async *extract(bytes, options) {
    const call = this.openExtract(options);

    for (let at = 0; at < bytes.length; at += CHUNK_BYTES) {
      call.write({ chunk: bytes.subarray(at, at + CHUNK_BYTES) });
    }
    call.end();

    // grpc-js hands events to callbacks; this turns the callback stream into
    // an async iterator without buffering the whole document's worth.
    const queue = [];
    let waiting = null;
    let done = false;
    let failure = null;

    const wake = () => {
      if (waiting) {
        const resolve = waiting;
        waiting = null;
        resolve();
      }
    };
    call.on("data", (event) => { queue.push(event); wake(); });
    call.on("end", () => { done = true; wake(); });
    call.on("error", (err) => { failure = err; done = true; wake(); });

    for (;;) {
      while (queue.length > 0) yield queue.shift();
      if (done) break;
      await new Promise((resolve) => { waiting = resolve; });
    }
    if (failure) throw failure;
  }

  /** Compile selectors without sending a document. */
  validateSelectors(rules) {
    return new Promise((resolve, reject) => {
      this.stub.validateSelectors({ rules }, (err, response) => {
        if (err) reject(err); else resolve(response);
      });
    });
  }

  close() {
    grpc.closeClient(this.stub);
  }
}

/**
 * Render one event as a single stable line.
 *
 * The format is deliberately terse and field-ordered so the Node, Python and
 * Java demos can be diffed against each other byte for byte. That agreement
 * is the point of having three of them.
 *
 * @param {object} response an ExtractResponse.
 * @returns {string|null} the line, or null for an event with nothing to say.
 */
export function formatEvent(response) {
  const { started, element, text, comment, doctype, endTag, finished, error } = response;

  if (started) {
    return `started encoding=${started.encoding} rules=${started.ruleCount}`;
  }
  if (element) {
    const attrs = element.attributes
      .map((a) => `${a.name}=${quote(a.value)}`)
      .join(" ");
    return [
      `element rule=${element.ruleId}`,
      `tag=${element.tagName}`,
      `ns=${element.namespace}`,
      `void=${!element.canHaveContent}`,
      `attrs=[${attrs}]`,
      span(element.span),
    ].filter(Boolean).join(" ");
  }
  if (text) {
    return [
      `text rule=${text.ruleId}`,
      `type=${text.textType}`,
      `last=${text.lastInNode}`,
      `value=${quote(text.text)}`,
      span(text.span),
    ].filter(Boolean).join(" ");
  }
  if (comment) {
    return `comment rule=${comment.ruleId} value=${quote(comment.text)}${suffix(span(comment.span))}`;
  }
  if (doctype) {
    return [
      `doctype rule=${doctype.ruleId}`,
      `name=${quote(doctype.name ?? "")}`,
      `public=${quote(doctype.publicId ?? "")}`,
      `system=${quote(doctype.systemId ?? "")}`,
    ].join(" ");
  }
  if (endTag) {
    return `endtag rule=${endTag.ruleId} tag=${endTag.name}${suffix(span(endTag.span))}`;
  }
  if (finished) {
    const counts = Object.keys(finished.matchesByRule).sort()
      .map((id) => `${id}:${finished.matchesByRule[id]}`)
      .join(",");
    return `finished bytes=${finished.bytesParsed} bailed=${finished.bailedOut} counts=[${counts}]`;
  }
  if (error) {
    return `error code=${error.code} message=${quote(firstLine(error.message))}`;
  }
  // An event this client has no name for. The contract says to ignore those
  // rather than fail: the oneof is the extension point.
  return null;
}

function span(value) {
  return value ? `span=${value.start}..${value.end}` : "";
}

function suffix(value) {
  return value ? ` ${value}` : "";
}

function firstLine(message) {
  return message.split("\n", 1)[0];
}

/**
 * Quote a string the same way all three demo clients do.
 *
 * Deliberately not `JSON.stringify`: the three languages disagree about
 * non-ASCII escaping, and the demos are compared byte for byte.
 */
function quote(value) {
  let out = '"';
  for (const ch of value) {
    if (ch === "\\") out += "\\\\";
    else if (ch === '"') out += '\\"';
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else out += ch;
  }
  return out + '"';
}
