// SPDX-License-Identifier: Apache-2.0
package demo;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.ByteString;
import lolhtml.v1.LolHtmlServiceGrpc;
import lolhtml.v1.Attribute;
import lolhtml.v1.Capture;
import lolhtml.v1.DocumentRule;
import lolhtml.v1.ExtractOptions;
import lolhtml.v1.ExtractRequest;
import lolhtml.v1.ExtractResponse;
import lolhtml.v1.ExtractRule;
import lolhtml.v1.SelectorDiagnostic;
import lolhtml.v1.SourceSpan;
import lolhtml.v1.TextType;
import lolhtml.v1.ValidateSelectorsRequest;
import lolhtml.v1.ValidateSelectorsResponse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Streams a local HTML file through grpc-lol-html and prints one line per event.
 *
 * <pre>
 *   mvn -q compile exec:java -Dexec.args="../sample-data/cdata_svg.html a[href]"
 * </pre>
 *
 * The output format matches the Node and Python demos byte for byte; that
 * agreement is the point of having three of them.
 */
public final class LolHtmlDemo {

    private static final int CHUNK_BYTES = 64 * 1024;

    /**
     * How long one call may take, end to end.
     *
     * <p>Without a deadline a call waits for as long as the server does, and
     * so does the latch that waits for it, so a server that stalls mid-stream
     * hangs this process forever. A minute is far more than any document a
     * demo is handed needs, and it still bounds the wait.
     */
    static final Duration DEADLINE = Duration.ofSeconds(60);

    /**
     * How much longer than the deadline to wait for the call to report that
     * it has failed. The deadline fails the call on time; this is only for a
     * transport that cannot deliver even that, which must not hang the demo
     * either.
     */
    private static final Duration DEADLINE_GRACE = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        List<String> argv = Arrays.asList(args);
        Set<String> flags = argv.stream().filter(a -> a.startsWith("--")).collect(Collectors.toSet());
        List<String> positional = argv.stream().filter(a -> !a.startsWith("--")).toList();

        if (positional.isEmpty()) {
            System.err.println("usage: LolHtmlDemo <file.html> [selector...] [--spans] "
                    + "[--script-text] [--all-text] [--raw-text] [--allow-ambiguous] "
                    + "[--encoding=LABEL]");
            System.exit(2);
        }

        List<Capture> captures = new ArrayList<>(List.of(
                Capture.CAPTURE_TAG_NAME,
                Capture.CAPTURE_ATTRIBUTES,
                Capture.CAPTURE_TEXT,
                Capture.CAPTURE_COMMENTS,
                Capture.CAPTURE_END_TAG));
        if (flags.contains("--spans")) {
            captures.add(Capture.CAPTURE_SOURCE_LOCATION);
        }

        List<String> selectors = positional.subList(1, positional.size());
        List<ExtractRule> rules = new ArrayList<>();
        if (selectors.isEmpty()) {
            rules.add(rule("title", "title", captures));
            rules.add(rule("links", "a[href]", captures));
            rules.add(rule("headings", "h1, h2, h3", captures));
        } else {
            for (int i = 0; i < selectors.size(); i++) {
                rules.add(rule("r" + i, selectors.get(i), captures));
            }
        }

        String encoding = argv.stream()
                .filter(a -> a.startsWith("--encoding="))
                .map(a -> a.substring("--encoding=".length()))
                .findFirst()
                .orElse("");

        ExtractOptions.Builder options = ExtractOptions.newBuilder()
                .addAllRules(rules)
                .setDocumentRule(DocumentRule.newBuilder()
                        .setId("doc").setDoctype(true).setComments(true).setText(false))
                .setEncoding(encoding)
                .setAdjustCharsetOnMetaTag(true)
                .setAllowAmbiguousMarkup(flags.contains("--allow-ambiguous"))
                .setRawTextChunks(flags.contains("--raw-text"));
        // No text types means the server's default of prose plus titles.
        // `--script-text` adds the two that make up most of a real page's
        // bytes; `--all-text` adds the last two as well, which only turn up in
        // `<plaintext>` and in CDATA sections inside foreign content.
        if (flags.contains("--all-text")) {
            options.addAllTextTypes(List.of(
                    TextType.TEXT_TYPE_DATA,
                    TextType.TEXT_TYPE_RCDATA,
                    TextType.TEXT_TYPE_RAW_TEXT,
                    TextType.TEXT_TYPE_SCRIPT_DATA,
                    TextType.TEXT_TYPE_PLAIN_TEXT,
                    TextType.TEXT_TYPE_CDATA_SECTION));
        } else if (flags.contains("--script-text")) {
            options.addAllTextTypes(List.of(
                    TextType.TEXT_TYPE_DATA,
                    TextType.TEXT_TYPE_RCDATA,
                    TextType.TEXT_TYPE_SCRIPT_DATA,
                    TextType.TEXT_TYPE_RAW_TEXT));
        }

        String address = System.getenv().getOrDefault("LOL_HTML_ADDR", "127.0.0.1:50057");
        ManagedChannel channel =
                Grpc.newChannelBuilder(address, InsecureChannelCredentials.create()).build();

        try {
            // Selector mistakes are the most common way to get an empty result,
            // and checking costs one cheap round trip against no upload at all.
            if (validate(channel, rules, DEADLINE) != 0) {
                System.exit(1);
            }

            byte[] document = Files.readAllBytes(Path.of(positional.get(0)));
            int exit = extract(channel, document, options.build(), DEADLINE);
            System.exit(exit);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Check the rules' selectors, reporting any that do not compile. Returns
     * the exit code: zero when every selector compiled.
     */
    static int validate(Channel channel, List<ExtractRule> rules, Duration deadline) {
        ValidateSelectorsResponse report;
        try {
            report = LolHtmlServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                    .validateSelectors(ValidateSelectorsRequest.newBuilder().addAllRules(rules).build());
        } catch (StatusRuntimeException e) {
            System.err.println("rpc failed: " + describe(e.getStatus()));
            return 1;
        }
        for (SelectorDiagnostic d : report.getDiagnosticsList()) {
            System.err.printf("bad selector in rule %s: %s%n  %s: %s%n",
                    d.getRuleId(), d.getSelector(), d.getCode(), d.getMessage());
        }
        return report.getDiagnosticsCount() > 0 ? 1 : 0;
    }

    /**
     * Run one document through Extract, printing each event as it arrives.
     * Returns the exit code: zero when the call completed.
     */
    static int extract(Channel channel, byte[] document, ExtractOptions options, Duration deadline)
            throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        StringBuilder failure = new StringBuilder();

        StreamObserver<ExtractRequest> requests = LolHtmlServiceGrpc.newStub(channel)
                .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS)
                .extract(new StreamObserver<>() {
                    @Override
                    public void onNext(ExtractResponse response) {
                        String line = format(response);
                        if (line != null) {
                            System.out.println(line);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.append(t instanceof StatusRuntimeException e
                                ? describe(e.getStatus()) : t.getMessage());
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });

        // Options first, always. The server validates them before opening the
        // response stream, so nothing comes back until it has them.
        requests.onNext(ExtractRequest.newBuilder().setOptions(options).build());
        for (int at = 0; at < document.length; at += CHUNK_BYTES) {
            int end = Math.min(at + CHUNK_BYTES, document.length);
            requests.onNext(ExtractRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(document, at, end - at))
                    .build());
        }
        requests.onCompleted();

        // Read on gRPC's threads while the upload above went out, which is
        // what the server needs: it holds only so much for a client that is
        // not reading. The deadline ends the call, and this wait, on time.
        if (!done.await(deadline.plus(DEADLINE_GRACE).toMillis(), TimeUnit.MILLISECONDS)) {
            requests.onError(Status.CANCELLED
                    .withDescription("no answer within the deadline").asRuntimeException());
            System.err.println("rpc failed: no answer within " + deadline);
            return 1;
        }
        if (failure.length() > 0) {
            System.err.println("rpc failed: " + failure);
            return 1;
        }
        return 0;
    }

    /** A status as one line: its code, then what the server or library said. */
    private static String describe(Status status) {
        return status.getDescription() == null
                ? status.getCode().toString()
                : status.getCode() + ": " + status.getDescription();
    }

    private static ExtractRule rule(String id, String selector, List<Capture> captures) {
        return ExtractRule.newBuilder()
                .setId(id).setSelector(selector).addAllCaptures(captures).build();
    }

    /** Render one event as a single stable line, or null to ignore it. */
    private static String format(ExtractResponse response) {
        switch (response.getEventCase()) {
            case STARTED -> {
                var e = response.getStarted();
                return "started encoding=" + e.getEncoding() + " rules=" + e.getRuleCount();
            }
            case ELEMENT -> {
                var e = response.getElement();
                String attrs = e.getAttributesList().stream()
                        .map(a -> a.getName() + "=" + quote(a.getValue()))
                        .collect(Collectors.joining(" "));
                return join("element rule=" + e.getRuleId(),
                        "tag=" + e.getTagName(),
                        "ns=" + e.getNamespace(),
                        "void=" + !e.getCanHaveContent(),
                        "attrs=[" + attrs + "]",
                        e.hasSpan() ? span(e.getSpan()) : "");
            }
            case TEXT -> {
                var e = response.getText();
                return join("text rule=" + e.getRuleId(),
                        "type=" + e.getTextType(),
                        "last=" + e.getLastInNode(),
                        "value=" + quote(e.getText()),
                        e.hasSpan() ? span(e.getSpan()) : "");
            }
            case COMMENT -> {
                var e = response.getComment();
                return join("comment rule=" + e.getRuleId(),
                        "value=" + quote(e.getText()),
                        e.hasSpan() ? span(e.getSpan()) : "");
            }
            case DOCTYPE -> {
                var e = response.getDoctype();
                return "doctype rule=" + e.getRuleId()
                        + " name=" + quote(e.hasName() ? e.getName() : "")
                        + " public=" + quote(e.hasPublicId() ? e.getPublicId() : "")
                        + " system=" + quote(e.hasSystemId() ? e.getSystemId() : "");
            }
            case END_TAG -> {
                var e = response.getEndTag();
                return join("endtag rule=" + e.getRuleId(),
                        "tag=" + e.getName(),
                        e.hasSpan() ? span(e.getSpan()) : "");
            }
            case FINISHED -> {
                var e = response.getFinished();
                String counts = new TreeMap<>(e.getMatchesByRuleMap()).entrySet().stream()
                        .map(entry -> entry.getKey() + ":" + entry.getValue())
                        .collect(Collectors.joining(","));
                return "finished bytes=" + e.getBytesParsed()
                        + " bailed=" + e.getBailedOut()
                        + " counts=[" + counts + "]";
            }
            case ERROR -> {
                var e = response.getError();
                String firstLine = e.getMessage().split("\n", 2)[0];
                return "error code=" + e.getCode() + " message=" + quote(firstLine);
            }
            // An event this client has no name for. The contract says to
            // ignore those rather than fail: the oneof is the extension point.
            default -> {
                return null;
            }
        }
    }

    private static String join(String... parts) {
        return Arrays.stream(parts).filter(p -> !p.isEmpty()).collect(Collectors.joining(" "));
    }

    private static String span(SourceSpan value) {
        return "span=" + value.getStart() + ".." + value.getEnd();
    }

    /**
     * Quote a string the same way all three demo clients do.
     *
     * <p>Deliberately hand-rolled rather than a JSON library: the three
     * languages disagree about non-ASCII escaping, and the demos are compared
     * byte for byte.
     */
    private static String quote(String value) {
        StringBuilder out = new StringBuilder("\"");
        value.codePoints().forEach(cp -> {
            switch (cp) {
                case '\\' -> out.append("\\\\");
                case '"' -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> out.appendCodePoint(cp);
            }
        });
        return out.append('"').toString();
    }

    private LolHtmlDemo() {
    }
}
