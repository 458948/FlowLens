package io.flowlens.ingest;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/** Bounded gzip ingestion helpers. Size limits raise dedicated exceptions. */
public final class GzipLimiter {

    private GzipLimiter() {
    }

    public static final class CompressedLimitExceededException extends IOException {
        public CompressedLimitExceededException(long max) {
            super("compressed body exceeds " + max + " bytes");
        }
    }

    public static final class DecompressedLimitExceededException extends IOException {
        public DecompressedLimitExceededException(long max) {
            super("decompressed body exceeds " + max + " bytes");
        }
    }

    /** Wraps a raw input stream so that compressed bytes are bounded. */
    public static InputStream boundedCompressed(InputStream in, int maxBytes) {
        return new FilterInputStream(in) {
            private long count;

            @Override
            public int read() throws IOException {
                int b = in.read();
                if (b != -1 && ++count > maxBytes) {
                    throw new CompressedLimitExceededException(maxBytes);
                }
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int n = in.read(b, off, len);
                if (n > 0) {
                    count += n;
                    if (count > maxBytes) {
                        throw new CompressedLimitExceededException(maxBytes);
                    }
                }
                return n;
            }
        };
    }

    /** Wraps a GZIPInputStream so that the decompressed size is bounded. */
    public static InputStream limitedGzip(InputStream in, int maxDecompressedBytes) throws IOException {
        return new LimitedInputStream(new GZIPInputStream(in, 8192), maxDecompressedBytes);
    }

    /** Reads at most maxBytes, throwing if the stream is larger. Kept for tests. */
    public static byte[] readCompressedLimited(InputStream in, int maxBytes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
        byte[] buf = new byte[8192];
        int total = 0;
        int n;
        while ((n = in.read(buf)) != -1) {
            total += n;
            if (total > maxBytes) {
                throw new IOException("compressed body exceeds " + maxBytes + " bytes");
            }
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private static final class LimitedInputStream extends FilterInputStream {
        private final long max;
        private long count;

        LimitedInputStream(InputStream in, long max) {
            super(in);
            this.max = max;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1 && ++count > max) {
                throw new DecompressedLimitExceededException(max);
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                count += n;
                if (count > max) {
                    throw new DecompressedLimitExceededException(max);
                }
            }
            return n;
        }
    }
}