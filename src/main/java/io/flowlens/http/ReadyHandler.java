package io.flowlens.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.flowlens.runtime.FlowLensRuntime;
import java.io.IOException;

/** GET /readyz. Reports the real runtime readiness state. */
final class ReadyHandler implements HttpHandler {

    private final FlowLensRuntime runtime;

    ReadyHandler(FlowLensRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        boolean running = runtime.state() == FlowLensRuntime.State.RUNNING;
        boolean degraded = runtime.isDegraded();
        boolean ready = running && !degraded;

        byte[] body = HttpJson.build(g -> {
            g.writeStartObject();
            g.writeBooleanField("ready", ready);
            g.writeStringField("state", runtime.state().name());
            g.writeBooleanField("degraded", degraded);
            g.writeEndObject();
        });
        HttpJson.send(exchange, ready ? 200 : 503, body);
    }
}