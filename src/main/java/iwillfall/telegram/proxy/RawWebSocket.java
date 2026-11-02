package iwillfall.telegram.proxy;

import java.io.*;
import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public final class RawWebSocket implements AutoCloseable {
    public final NettyStream stream;
    private boolean closed;
    private final ByteArrayOutputStream fragments = new ByteArrayOutputStream();

    public static final class HandshakeError extends IOException {
        public final int status;
        public final Map<String, String> headers;

        HandshakeError(int status, String line, Map<String, String> headers) {
            super(line);
            this.status = status;
            this.headers = headers;
        }
    }

    private RawWebSocket(NettyStream stream) {
        this.stream = stream;
    }

    public static CompletableFuture<RawWebSocket> connect(EventLoopManager loops, Config config, String host,
            String domain, String path, String sni, boolean secure, int timeout) {
        return NettyStream.connect(loops, config, host, secure ? 443 : 80,
                secure ? (sni == null ? domain : sni) : null, sni != null).thenCompose(s -> {
                    String req = "GET " + path + " HTTP/1.1\r\nHost: " + domain
                            + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: "
                            + Base64.getEncoder().encodeToString(Utils.random(16))
                            + "\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: binary\r\n\r\n";
                    CompletableFuture<RawWebSocket> f = s.write(req.getBytes(StandardCharsets.US_ASCII))
                            .thenCompose(v -> headers(s, new ArrayList<>()));
                    f.orTimeout(timeout, TimeUnit.SECONDS).whenComplete((w, e) -> {
                        if (e != null) {
                            s.close();
                        }
                    });
                    return f;
                });
    }

    private static CompletableFuture<RawWebSocket> headers(NettyStream s, List<String> lines) {
        return s.line().thenCompose(b -> {
            String line = new String(b, StandardCharsets.UTF_8).trim();
            if (!line.isEmpty()) {
                if (lines.size() > 256) {
                    return Utils.failed(new IOException("Too many HTTP headers"));
                }
                lines.add(line);
                return headers(s, lines);
            }
            if (lines.isEmpty()) {
                return Utils.failed(new HandshakeError(0, "Empty response", Map.of()));
            }
            String[] parts = lines.get(0).split(" ", 3);
            int status = 0;
            try {
                status = Integer.parseInt(parts[1]);
            } catch (RuntimeException ignored) {
            }
            if (status == 101) {
                return CompletableFuture.completedFuture(new RawWebSocket(s));
            }
            Map<String, String> hs = new HashMap<>();
            for (String l : lines) {
                int colon = l.indexOf(':');
                if (colon > 0) {
                    hs.put(l.substring(0, colon).trim().toLowerCase(Locale.ROOT), l.substring(colon + 1).trim());
                }
            }
            return Utils.failed(new HandshakeError(status, lines.get(0), hs));
        });
    }

    public CompletableFuture<Void> send(byte[] b) {
        if (closed) {
            return Utils.failed(new IOException("WebSocket closed"));
        }
        return stream.write(frame(2, b));
    }

    public CompletableFuture<Void> sendBatch(List<byte[]> parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            out.writeBytes(frame(2, p));
        }
        return stream.write(out.toByteArray());
    }

    public static byte[] frame(int op, byte[] data) {
        int n = data.length;
        ByteBuffer b = ByteBuffer.allocate(n + 14);
        b.put((byte) (128 | op));
        if (n < 126) {
            b.put((byte) (128 | n));
        } else if (n < 65536) {
            b.put((byte) 254).putShort((short) n);
        } else {
            b.put((byte) 255).putLong(n);
        }
        byte[] mask = Utils.random(4);
        b.put(mask);
        for (int i = 0; i < n; i++) {
            b.put((byte) (data[i] ^ mask[i % 4]));
        }
        return Arrays.copyOf(b.array(), b.position());
    }

    public CompletableFuture<byte[]> recv() {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        return stream.read(2).thenCompose(h -> {
            int len = h[1] & 127;
            CompletableFuture<byte[]> ext = len == 126 ? stream.read(2)
                    : len == 127 ? stream.read(8) : CompletableFuture.completedFuture(new byte[0]);
            return ext.thenCompose(x -> {
                long n = len < 126 ? len
                        : len == 126 ? (ByteBuffer.wrap(x).getShort() & 65535) : ByteBuffer.wrap(x).getLong();
                if (n < 0 || n > 16 * 1024 * 1024) {
                    return Utils.failed(new IOException("WS frame too large"));
                }
                boolean masked = (h[1] & 128) != 0;
                return (masked ? stream.read(4) : CompletableFuture.completedFuture(new byte[0]))
                        .thenCompose(mask -> (n == 0 ? CompletableFuture.completedFuture(new byte[0])
                                : stream.read((int) n)).thenCompose(payload -> {
                                    if (masked) {
                                        for (int i = 0; i < payload.length; i++) {
                                            payload[i] ^= mask[i % 4];
                                        }
                                    }
                                    int op = h[0] & 15;
                                    if (op == 8) {
                                        closed = true;
                                        return stream.write(frame(8, payload.length >= 2
                                                ? Utils.slice(payload, 0, 2) : new byte[0]))
                                                .handle((v, e) -> (byte[]) null);
                                    }
                                    if (op == 9) {
                                        return stream.write(frame(10, payload)).thenCompose(v -> recv());
                                    }
                                    if (op == 10) {
                                        return recv();
                                    }
                                    if (op != 0 && op != 1 && op != 2) {
                                        return recv();
                                    }
                                    boolean fin = (h[0] & 128) != 0;
                                    if (fin && fragments.size() == 0) {
                                        return CompletableFuture.completedFuture(payload);
                                    }
                                    fragments.writeBytes(payload);
                                    if (fragments.size() > 16 * 1024 * 1024) {
                                        return Utils.failed(new IOException("WS fragmented message too large"));
                                    }
                                    if (!fin) {
                                        return recv();
                                    }
                                    byte[] r = fragments.toByteArray();
                                    fragments.reset();
                                    return CompletableFuture.completedFuture(r);
                                }));
            });
        });
    }

    public boolean alive() {
        return !closed && stream.alive();
    }

    public void close() {
        if (!closed) {
            closed = true;
            stream.write(frame(8, new byte[0])).whenComplete((v, e) -> stream.close());
        } else {
            stream.close();
        }
    }
}
