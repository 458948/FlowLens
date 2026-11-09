package io.flowlens;

import io.flowlens.ingest.UrlNormalizer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UrlNormalizerTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void stripsQueryAndFragment() {
        assertArrayEquals(b("/a/b"), UrlNormalizer.normalize(b("/a/b?x=1#frag"), 0, "/a/b?x=1#frag".length()));
    }

    @Test
    void lowercasesSchemeAndHostButKeepsPathCase() {
        assertArrayEquals(b("http://example.com/Path/User"),
                UrlNormalizer.normalize(b("HTTP://Example.COM/Path/User"), 0, "HTTP://Example.COM/Path/User".length()));
    }

    @Test
    void resolvesDotSegments() {
        assertArrayEquals(b("/a/c"),
                UrlNormalizer.normalize(b("/a/./b/../c"), 0, "/a/./b/../c".length()));
        assertArrayEquals(b("/b"),
                UrlNormalizer.normalize(b("/a/../b"), 0, "/a/../b".length()));
    }

    @Test
    void rejectsOverlongNormalizedUrl() {
        String longPath = "http://example.com/" + "a".repeat(300);
        assertThrows(UrlNormalizer.FieldTooLongException.class,
                () -> UrlNormalizer.normalize(b(longPath), 0, longPath.length()));
    }
}