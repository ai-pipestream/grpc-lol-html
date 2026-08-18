#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Stream a local HTML file through grpc-lol-html and print one line per event.

    ./run.sh ../sample-data/cdata_svg.html "a[href]"
    ./run.sh page.html "title" "h1, h2" --spans --script-text

With no selectors it uses a small default rule set: the title, every link, and
the top three heading levels.

The output format matches the Node and Java demos byte for byte; that
agreement is the point of having three of them.
"""

import os
import sys

import grpc

# protoc emits absolute imports (`from lolhtml.v1 import types_pb2`), so the
# generated tree has to be a root on sys.path rather than a package we import
# through.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))

from lolhtml.v1 import lolhtml_service_pb2 as svc  # noqa: E402
from lolhtml.v1 import lolhtml_service_pb2_grpc as svc_grpc  # noqa: E402
from lolhtml.v1 import types_pb2 as types  # noqa: E402

CHUNK_BYTES = 64 * 1024


def quote(value: str) -> str:
    """Quote a string the same way all three demo clients do.

    Deliberately not `json.dumps`: the three languages disagree about
    non-ASCII escaping, and the demos are compared byte for byte.
    """
    out = ['"']
    for ch in value:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def span(message, field: str = "span") -> str:
    if not message.HasField(field):
        return ""
    value = getattr(message, field)
    return f"span={value.start}..{value.end}"


def optional(message, field: str) -> str:
    return quote(getattr(message, field) if message.HasField(field) else "")


def format_event(response) -> str | None:
    """Render one event as a single stable line, or None to ignore it."""
    which = response.WhichOneof("event")

    if which == "started":
        e = response.started
        return f"started encoding={e.encoding} rules={e.rule_count}"

    if which == "element":
        e = response.element
        attrs = " ".join(f"{a.name}={quote(a.value)}" for a in e.attributes)
        parts = [
            f"element rule={e.rule_id}",
            f"tag={e.tag_name}",
            f"ns={types.Namespace.Name(e.namespace)}",
            f"void={str(not e.can_have_content).lower()}",
            f"attrs=[{attrs}]",
            span(e),
        ]
        return " ".join(p for p in parts if p)

    if which == "text":
        e = response.text
        parts = [
            f"text rule={e.rule_id}",
            f"type={types.TextType.Name(e.text_type)}",
            f"last={str(e.last_in_node).lower()}",
            f"value={quote(e.text)}",
            span(e),
        ]
        return " ".join(p for p in parts if p)

    if which == "comment":
        e = response.comment
        tail = span(e)
        return f"comment rule={e.rule_id} value={quote(e.text)}" + (f" {tail}" if tail else "")

    if which == "doctype":
        e = response.doctype
        return (
            f"doctype rule={e.rule_id} name={optional(e, 'name')} "
            f"public={optional(e, 'public_id')} system={optional(e, 'system_id')}"
        )

    if which == "end_tag":
        e = response.end_tag
        tail = span(e)
        return f"endtag rule={e.rule_id} tag={e.name}" + (f" {tail}" if tail else "")

    if which == "finished":
        e = response.finished
        counts = ",".join(f"{k}:{e.matches_by_rule[k]}" for k in sorted(e.matches_by_rule))
        return (
            f"finished bytes={e.bytes_parsed} "
            f"bailed={str(e.bailed_out).lower()} counts=[{counts}]"
        )

    if which == "error":
        e = response.error
        first_line = e.message.split("\n", 1)[0]
        return f"error code={types.ParseErrorCode.Name(e.code)} message={quote(first_line)}"

    # An event this client has no name for. The contract says to ignore those
    # rather than fail: the oneof is the extension point.
    return None


def frames(data: bytes, options):
    """Yield the request stream: options first, then the document."""
    yield svc.ExtractRequest(options=options)
    for at in range(0, len(data), CHUNK_BYTES):
        yield svc.ExtractRequest(chunk=data[at : at + CHUNK_BYTES])


def main() -> int:
    argv = sys.argv[1:]
    flags = {a for a in argv if a.startswith("--")}
    positional = [a for a in argv if not a.startswith("--")]

    if not positional:
        print(
            "usage: client.py <file.html> [selector...] [--spans] [--script-text] "
            "[--all-text] [--raw-text] [--allow-ambiguous] [--encoding=LABEL]",
            file=sys.stderr,
        )
        return 2

    path, selectors = positional[0], positional[1:]

    captures = [
        types.CAPTURE_TAG_NAME,
        types.CAPTURE_ATTRIBUTES,
        types.CAPTURE_TEXT,
        types.CAPTURE_COMMENTS,
        types.CAPTURE_END_TAG,
    ]
    if "--spans" in flags:
        captures.append(types.CAPTURE_SOURCE_LOCATION)

    if selectors:
        rules = [
            types.ExtractRule(id=f"r{i}", selector=s, captures=captures)
            for i, s in enumerate(selectors)
        ]
    else:
        rules = [
            types.ExtractRule(id="title", selector="title", captures=captures),
            types.ExtractRule(id="links", selector="a[href]", captures=captures),
            types.ExtractRule(id="headings", selector="h1, h2, h3", captures=captures),
        ]

    encoding = next(
        (a[len("--encoding=") :] for a in argv if a.startswith("--encoding=")), ""
    )
    # Empty means the server's default of prose plus titles. `--script-text`
    # adds the two that make up most of a real page's bytes; `--all-text` adds
    # the last two as well, which only turn up in `<plaintext>` and in CDATA
    # sections inside foreign content.
    if "--all-text" in flags:
        text_types = [
            types.TEXT_TYPE_DATA,
            types.TEXT_TYPE_RCDATA,
            types.TEXT_TYPE_RAW_TEXT,
            types.TEXT_TYPE_SCRIPT_DATA,
            types.TEXT_TYPE_PLAIN_TEXT,
            types.TEXT_TYPE_CDATA_SECTION,
        ]
    elif "--script-text" in flags:
        text_types = [
            types.TEXT_TYPE_DATA,
            types.TEXT_TYPE_RCDATA,
            types.TEXT_TYPE_SCRIPT_DATA,
            types.TEXT_TYPE_RAW_TEXT,
        ]
    else:
        text_types = []

    options = svc.ExtractOptions(
        rules=rules,
        document_rule=types.DocumentRule(id="doc", doctype=True, comments=True, text=False),
        encoding=encoding,
        adjust_charset_on_meta_tag=True,
        allow_ambiguous_markup="--allow-ambiguous" in flags,
        raw_text_chunks="--raw-text" in flags,
        text_types=text_types,
    )

    address = os.environ.get("LOL_HTML_ADDR", "127.0.0.1:50053")
    with grpc.insecure_channel(address) as channel:
        stub = svc_grpc.LolHtmlServiceStub(channel)

        # Selector mistakes are the most common way to get an empty result,
        # and checking costs one cheap round trip against no upload at all.
        report = stub.ValidateSelectors(svc.ValidateSelectorsRequest(rules=rules))
        if report.diagnostics:
            for d in report.diagnostics:
                print(
                    f"bad selector in rule {d.rule_id}: {d.selector}\n"
                    f"  {types.SelectorErrorCode.Name(d.code)}: {d.message}",
                    file=sys.stderr,
                )
            return 1

        with open(path, "rb") as handle:
            data = handle.read()

        try:
            for response in stub.Extract(frames(data, options)):
                line = format_event(response)
                if line is not None:
                    print(line)
        except grpc.RpcError as err:
            print(f"rpc failed: {err.details()}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
