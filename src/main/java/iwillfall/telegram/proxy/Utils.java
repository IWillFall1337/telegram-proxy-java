package iwillfall.telegram.proxy;

import io.netty.handler.ssl.*;
import java.nio.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.*;

public final class Utils {
    public static final int ABRIDGED = 0xefefefef, INTERMEDIATE = 0xeeeeeeee, SECURE = 0xdddddddd;
    public static final Map<Integer, String> DC = Map.of(1, "149.154.175.50", 2, "149.154.167.51",
            3, "149.154.175.100", 4, "149.154.167.91", 5, "149.154.171.5", 203, "91.105.192.100");
    public static final Map<Integer, String> TEST = Map.of(1, "149.154.175.10", 2, "149.154.167.40", 3, "149.154.175.117");
    public static final SecureRandom RANDOM = new SecureRandom();

    private Utils() {
    }

    public static byte[] random(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    public static byte[] slice(byte[] b, int a, int z) {
        return Arrays.copyOfRange(b, a, z);
    }

    public static byte[] reverse(byte[] b) {
        b = b.clone();
        for (int i = 0, j = b.length - 1; i < j; i++, j--) {
            byte v = b[i];
            b[i] = b[j];
            b[j] = v;
        }
        return b;
    }

    public static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) {
            n += p.length;
        }
        byte[] b = new byte[n];
        int o = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, b, o, p.length);
            o += p.length;
        }
        return b;
    }

    public static int le(byte[] b, int o) {
        return ByteBuffer.wrap(b, o, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static byte[] le(int n) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(n).array();
    }

    public static byte[] sha(byte[]... b) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(concat(b));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac m = Mac.getInstance("HmacSHA256");
            m.init(new SecretKeySpec(key, "HmacSHA256"));
            return m.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static double now() {
        return System.nanoTime() / 1e9;
    }

    public static String human(long n) {
        double v = n;
        for (String u : List.of("B", "KB", "MB", "GB", "TB")) {
            if (v < 1024 || u.equals("TB")) {
                return String.format(Locale.ROOT, "%.1f%s", v, u);
            }
            v /= 1024;
        }
        return "";
    }

    public static List<String> wsDomains(int dc, boolean media) {
        if (dc == 203) {
            dc = 2;
        }
        return media ? List.of("kws" + dc + "-1.web.telegram.org", "kws" + dc + ".web.telegram.org")
                : List.of("kws" + dc + ".web.telegram.org");
    }

    public static SslHandler ssl(io.netty.buffer.ByteBufAllocator alloc, String host, boolean front,
            boolean h2) throws Exception {
        SslContextBuilder b = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);
        if (h2) {
            b.applicationProtocolConfig(new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT, "h2"));
        }
        SslHandler s = b.build().newHandler(alloc, host, 443);
        SSLParameters p = s.engine().getSSLParameters();
        p.setEndpointIdentificationAlgorithm(front ? null : "HTTPS");
        p.setServerNames(List.of(new SNIHostName(host)));
        s.engine().setSSLParameters(p);
        s.setHandshakeTimeoutMillis(8000);
        return s;
    }

    public static String githubText(java.net.URI uri) throws java.io.IOException {
        String host = uri.getHost();
        if (Set.of("raw.githubusercontent.com", "release-assets.githubusercontent.com").contains(host)) {
            try (java.net.Socket tcp = new java.net.Socket()) {
                tcp.connect(new java.net.InetSocketAddress("185.199.109.133", 443), 10000);
                tcp.setSoTimeout(10000);
                try (SSLSocket tls = (SSLSocket) ((SSLSocketFactory) SSLSocketFactory.getDefault())
                        .createSocket(tcp, host, 443, true)) {
                    SSLParameters p = tls.getSSLParameters();
                    p.setEndpointIdentificationAlgorithm("HTTPS");
                    p.setServerNames(List.of(new SNIHostName(host)));
                    tls.setSSLParameters(p);
                    tls.startHandshake();
                    tls.getOutputStream().write(("GET " + uri.getRawPath()
                            + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery())
                            + " HTTP/1.0\r\nHost: " + host
                            + "\r\nUser-Agent: tg-ws-proxy\r\nConnection: close\r\n\r\n")
                            .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(
                            tls.getInputStream(), java.nio.charset.StandardCharsets.UTF_8));
                    String status = reader.readLine();
                    if (status == null || !status.matches("HTTP/1\\.[01] 200.*")) {
                        throw new java.io.IOException("GitHub HTTP status: " + status);
                    }
                    String line;
                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    }
                    StringBuilder body = new StringBuilder();
                    while ((line = reader.readLine()) != null) {
                        body.append(line).append('\n');
                    }
                    return body.toString();
                }
            } catch (java.io.IOException e) {
                /* Retry through normal DNS with the same TLS hostname verification. */
            }
        }
        java.net.URLConnection connection = uri.toURL().openConnection(java.net.Proxy.NO_PROXY);
        connection.setConnectTimeout(10000);
        connection.setReadTimeout(10000);
        connection.setRequestProperty("User-Agent", "tg-ws-proxy");
        try (java.io.InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    public static <T> CompletableFuture<T> failed(Throwable e) {
        return CompletableFuture.failedFuture(e);
    }
}
