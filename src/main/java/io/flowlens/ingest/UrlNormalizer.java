package io.flowlens.ingest;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * URL normalization: strip query and fragment, lowercase scheme and host,
 * resolve dot segments, keep path case. Over-long results are rejected rather
 * than truncated.
 */
public final class UrlNormalizer {

    public static final int MAX_RAW_URL_BYTES = 1024;
    public static final int MAX_NORMALIZED_BYTES = 256;
    public static final int MAX_EVENT_ID_BYTES = 128;
    public static final int MAX_USER_ID_BYTES = 64;

    private UrlNormalizer() {
    }

    public static byte[] normalize(byte[] raw, int off, int len) {
        if (len > MAX_RAW_URL_BYTES) {
            throw new FieldTooLongException("raw url exceeds " + MAX_RAW_URL_BYTES + " bytes");
        }
        String s = new String(raw, off, len, StandardCharsets.UTF_8);

        int hash = s.indexOf('#');
        if (hash >= 0) {
            s = s.substring(0, hash);
        }
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q);
        }

        String result;
        int schemeIdx = s.indexOf("://");
        if (schemeIdx >= 0) {
            String scheme = s.substring(0, schemeIdx).toLowerCase(Locale.ROOT);
            String rest = s.substring(schemeIdx + 3);
            int slash = rest.indexOf('/');
            String authority;
            String path;
            if (slash >= 0) {
                authority = rest.substring(0, slash);
                path = rest.substring(slash);
            } else {
                authority = rest;
                path = "";
            }
            authority = authority.toLowerCase(Locale.ROOT);
            result = scheme + "://" + authority + removeDotSegments(path);
        } else {
            result = removeDotSegments(s);
        }

        byte[] out = result.getBytes(StandardCharsets.UTF_8);
        if (out.length > MAX_NORMALIZED_BYTES) {
            throw new FieldTooLongException("normalized url exceeds " + MAX_NORMALIZED_BYTES + " bytes");
        }
        return out;
    }

    private static String removeDotSegments(String path) {
        boolean leadingSlash = path.startsWith("/");
        String[] parts = path.split("/", -1);
        String[] stack = new String[parts.length];
        int top = 0;
        for (String part : parts) {
            if (part.isEmpty() || part.equals(".")) {
                continue;
            }
            if (part.equals("..")) {
                if (top > 0 && !stack[top - 1].equals("..")) {
                    top--;
                } else if (!leadingSlash) {
                    stack[top++] = "..";
                }
                continue;
            }
            stack[top++] = part;
        }
        StringBuilder sb = new StringBuilder(path.length());
        if (leadingSlash) {
            sb.append('/');
        }
        for (int i = 0; i < top; i++) {
            if (i > 0) {
                sb.append('/');
            }
            sb.append(stack[i]);
        }
        if (top > 0 && path.endsWith("/")) {
            sb.append('/');
        }
        return sb.toString();
    }

    public static final class FieldTooLongException extends IllegalArgumentException {
        public FieldTooLongException(String message) {
            super(message);
        }
    }
}