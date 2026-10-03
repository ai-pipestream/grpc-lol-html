// SPDX-License-Identifier: Apache-2.0
package demo;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import lolhtml.v1.Capture;
import lolhtml.v1.ExtractOptions;
import lolhtml.v1.ExtractRequest;
import lolhtml.v1.ExtractResponse;
import lolhtml.v1.ExtractRule;
import lolhtml.v1.LolHtmlServiceGrpc;
import lolhtml.v1.ValidateSelectorsRequest;
import lolhtml.v1.ValidateSelectorsResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A server that takes a call and never answers must not hang the demo.
 *
 * <p>The server here accepts both calls and then says nothing, which is what
 * a wedged server looks like from the client's side. Before the demo set
 * deadlines, each of these waited forever, which the class timeout turns
 * into a failure rather than a hung build.
 */
@Timeout(60)
class LolHtmlDemoTest {

    private static final Duration SHORT_DEADLINE = Duration.ofMillis(300);

    /**
     * Comfortably past the deadline, and well short of the deadline plus the
     * demo's grace period: a call that ends inside this ended because of its
     * deadline, not because the demo stopped waiting for it.
     */
    private static final Duration ENDED_BY_THE_DEADLINE = Duration.ofSeconds(3);

    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void startASilentServer() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new LolHtmlServiceGrpc.LolHtmlServiceImplBase() {
                    @Override
                    public StreamObserver<ExtractRequest> extract(
                            StreamObserver<ExtractResponse> responses) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ExtractRequest request) {
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                            }
                        };
                    }

                    @Override
                    public void validateSelectors(ValidateSelectorsRequest request,
                            StreamObserver<ValidateSelectorsResponse> responses) {
                    }
                })
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void anExtractTheServerNeverAnswersFailsAtTheDeadline() throws Exception {
        long started = System.nanoTime();
        int exit = LolHtmlDemo.extract(channel, "<p>x</p>".getBytes(StandardCharsets.UTF_8),
                ExtractOptions.newBuilder().addRules(rule()).build(), SHORT_DEADLINE);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(exit).isEqualTo(1);
        assertThat(elapsed).isLessThan(ENDED_BY_THE_DEADLINE);
    }

    @Test
    void aValidationTheServerNeverAnswersFailsAtTheDeadline() {
        long started = System.nanoTime();
        int exit = LolHtmlDemo.validate(channel, List.of(rule()), SHORT_DEADLINE);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(exit).isEqualTo(1);
        assertThat(elapsed).isLessThan(ENDED_BY_THE_DEADLINE);
    }

    private static ExtractRule rule() {
        return ExtractRule.newBuilder()
                .setId("p").setSelector("p").addCaptures(Capture.CAPTURE_TAG_NAME).build();
    }
}
