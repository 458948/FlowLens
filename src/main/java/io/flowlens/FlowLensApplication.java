package io.flowlens;

import io.flowlens.http.FlowLensHttpServer;
import io.flowlens.runtime.FlowLensRuntime;
import java.util.concurrent.CountDownLatch;

/** Startup entry point for FlowLens. */
public final class FlowLensApplication {

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("FLOWLENS_HOST", "127.0.0.1");
        String portRaw = System.getenv().getOrDefault("FLOWLENS_PORT", "8080");
        int port;
        try {
            port = Integer.parseInt(portRaw);
            if (port < 0 || port > 65535) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Invalid FLOWLENS_PORT: " + portRaw);
            System.exit(2);
            return;
        }

        FlowLensRuntime runtime = new FlowLensRuntime();
        FlowLensHttpServer server = new FlowLensHttpServer(runtime, host, port);
        server.start();
        System.out.println("FlowLens listening on http://" + host + ":" + server.port());

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            io.flowlens.http.FlowLensHttpServer.CloseResult result = server.close();
            System.out.println("FlowLens shutdown: drained=" + result.drained()
                    + " httpIncomplete=" + result.httpIncomplete()
                    + " handlersNotExited=" + result.handlersNotExited()
                    + " runtimeDrained=" + result.runtimeDrained()
                    + " unprocessed=" + result.unprocessedEvents());
            latch.countDown();
        }, "flowlens-shutdown"));

        latch.await();
    }
}