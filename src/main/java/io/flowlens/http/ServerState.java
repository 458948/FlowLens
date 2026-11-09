package io.flowlens.http;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared HTTP server lifecycle state. */
final class ServerState {

    final AtomicBoolean closing = new AtomicBoolean(false);
    final AtomicInteger inFlight = new AtomicInteger();

    boolean isClosing() {
        return closing.get();
    }

    int inFlight() {
        return inFlight.get();
    }
}