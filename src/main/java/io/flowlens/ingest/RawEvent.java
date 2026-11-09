package io.flowlens.ingest;

/** A parsed and validated ingest event. url holds the normalized URL bytes. */
public final class RawEvent {
    public final byte[] eventId;
    public final byte[] userId;
    public final byte[] url;
    public final long ts;
    public final long bytes;

    public RawEvent(byte[] eventId, byte[] userId, byte[] url, long ts, long bytes) {
        this.eventId = eventId;
        this.userId = userId;
        this.url = url;
        this.ts = ts;
        this.bytes = bytes;
    }
}