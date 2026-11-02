package iwillfall.telegram.proxy;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public final class Bridge {
    private final Config config;
    private final EventLoopManager loops;
    private final Stats stats;
    private final Balancer balancer;
    private final ConnectionPool pool;
    private final CloudflareH2Client h2;
    private final Map<String, Integer> tcpFailures = new HashMap<>();
    private final Map<String, Double> tcpRetry = new HashMap<>();
    private final Set<String> tcpConnecting = new HashSet<>();

    public Bridge(Config c, EventLoopManager l, Stats s, Balancer b, ConnectionPool p, CloudflareH2Client h) {
        config = c;
        loops = l;
        stats = s;
        balancer = b;
        pool = p;
        h2 = h;
    }

    public CompletableFuture<Void> route(ByteStream client, AesCipher.Handshake hs, byte[] secret, String label) {
        int dc = hs.dc() >= 10000 ? hs.dc() - 10000 : hs.dc();
        boolean test = config.forceTestDc || hs.dc() >= 10000;
        byte[] init = AesCipher.relay(hs.protocol(), hs.media() ? -dc : dc);
        AesCipher.Context crypto = AesCipher.context(hs.prekeyIv(), secret, init);
        RawWebSocket direct = pool.get(dc, hs.media(), test);
        if (direct != null) {
            stats.ws.incrementAndGet();
            return direct.send(init)
                    .thenCompose(v -> ws(client, direct, crypto, new MsgSplitter(hs.protocol()), label, dc))
                    .whenComplete((v, e) -> direct.close());
        }
        String dst = (test ? Utils.TEST : Utils.DC).get(dc);
        CompletableFuture<RawWebSocket> worker = CompletableFuture.completedFuture(null);
        if (dst != null && !config.workerDomains.isEmpty()) {
            RawWebSocket cached = test ? null : pool.worker(dc);
            worker = cached != null ? CompletableFuture.completedFuture(cached) : pool.connectWorker(dc, test);
        }
        return worker.thenCompose(w -> {
            if (w != null) {
                stats.cf.incrementAndGet();
                return w.send(init).thenCompose(v -> ws(client, w, crypto, null, label, dc))
                        .whenComplete((v, e) -> w.close());
            }
            return cfRoute(client, crypto, init, hs.protocol(), dc, hs.media(), test, dst, label);
        });
    }

    private CompletableFuture<Void> cfRoute(ByteStream client, AesCipher.Context ctx, byte[] init, int proto,
            int dc, boolean media, boolean test, String dst, String label) {
        CompletableFuture<CloudflareH2Client.HttpChannel> channel =
                !test && config.fallbackCfproxy && media && h2 != null
                        ? h2.open(dc) : CompletableFuture.completedFuture(null);
        return channel.thenCompose(h -> {
            if (h != null) {
                stats.cf.incrementAndGet();
                stats.h2.incrementAndGet();
                return h2Bridge(client, h, ctx, proto);
            }
            CompletableFuture<RawWebSocket> ws = !test && config.fallbackCfproxy
                    ? cfTry(dc, balancer.domains(dc), 0) : CompletableFuture.completedFuture(null);
            return ws.thenCompose(w -> {
                if (w != null) {
                    stats.cf.incrementAndGet();
                    return w.send(init).thenCompose(v -> ws(client, w, ctx, new MsgSplitter(proto), label, dc))
                            .whenComplete((v, e) -> w.close());
                }
                return tcp(client, ctx, init, dst);
            });
        });
    }

    private CompletableFuture<RawWebSocket> cfTry(int dc, List<String> ds, int i) {
        if (i >= ds.size()) {
            return CompletableFuture.completedFuture(null);
        }
        String d = "kws" + dc + "." + ds.get(i);
        return RawWebSocket.connect(loops, config, d, d, "/apiws", null, !config.disableSecure, 10)
                .handle((w, e) -> {
                    if (e == null) {
                        balancer.success(dc, ds.get(i));
                        return CompletableFuture.completedFuture(w);
                    }
                    return cfTry(dc, ds, i + 1);
                }).thenCompose(f -> f);
    }

    private CompletableFuture<Void> tcp(ByteStream client, AesCipher.Context ctx, byte[] init, String dst) {
        if (dst == null) {
            return Utils.failed(new IOException("No route for DC"));
        }
        synchronized (this) {
            if (tcpConnecting.contains(dst) || Utils.now() < tcpRetry.getOrDefault(dst, 0.0)) {
                return Utils.failed(new IOException("TCP fallback backoff"));
            }
            tcpConnecting.add(dst);
        }
        return NettyStream.connect(loops, config, dst, 443, null, false).handle((remote, e) -> {
            synchronized (this) {
                tcpConnecting.remove(dst);
                if (e != null) {
                    int n = tcpFailures.merge(dst, 1, Integer::sum);
                    tcpRetry.put(dst, Utils.now() + Math.min(3600, 30 * Math.pow(2, Math.min(n - 1, 7))));
                    return Utils.<Void>failed(e);
                }
                tcpFailures.remove(dst);
                tcpRetry.remove(dst);
            }
            stats.tcp.incrementAndGet();
            return remote.write(init).thenCompose(v -> duplex(client, remote, ctx))
                    .whenComplete((v, e2) -> remote.close());
        }).thenCompose(f -> f);
    }

    private CompletableFuture<Void> duplex(ByteStream client, ByteStream remote, AesCipher.Context ctx) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        forward(client, remote, ctx.cltDec(), ctx.tgEnc(), true, done);
        forward(remote, client, ctx.tgDec(), ctx.cltEnc(), false, done);
        return done.whenComplete((v, e) -> {
            remote.close();
            client.close();
        });
    }

    private void forward(ByteStream src, ByteStream dst, AesCipher dec, AesCipher enc, boolean up,
            CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        src.read(0).thenCompose(b -> {
            if (b.length == 0) {
                done.complete(null);
                return CompletableFuture.completedFuture(null);
            }
            if (up) {
                stats.up.addAndGet(b.length);
            } else {
                stats.down.addAndGet(b.length);
            }
            return dst.write(enc.update(dec.update(b)));
        }).whenComplete((v, e) -> {
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                forward(src, dst, dec, enc, up, done);
            }
        });
    }

    public static CompletableFuture<Void> relay(ByteStream a, ByteStream b) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        plain(a, b, done);
        plain(b, a, done);
        return done.whenComplete((v, e) -> {
            a.close();
            b.close();
        });
    }

    private static void plain(ByteStream a, ByteStream b, CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        a.read(0).thenCompose(x -> {
            if (x.length == 0) {
                done.complete(null);
                return CompletableFuture.completedFuture(null);
            }
            return b.write(x);
        }).whenComplete((v, e) -> {
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                plain(a, b, done);
            }
        });
    }

    private CompletableFuture<Void> ws(ByteStream client, RawWebSocket remote, AesCipher.Context ctx,
            MsgSplitter splitter, String label, int dc) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        NetworkDebug activity = new NetworkDebug(label, "DC" + dc);
        wsUp(client, remote, ctx, splitter, activity, done);
        wsDown(client, remote, ctx, activity, done);
        return done.whenComplete((v, e) -> {
            activity.close();
            remote.close();
            client.close();
        });
    }

    private void wsUp(ByteStream client, RawWebSocket remote, AesCipher.Context ctx, MsgSplitter splitter,
            NetworkDebug activity, CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        client.read(0).thenCompose(b -> {
            if (b.length == 0) {
                byte[] tail = splitter == null ? new byte[0] : splitter.flush();
                return (tail.length > 0 ? remote.send(tail) : CompletableFuture.<Void>completedFuture(null))
                        .thenRun(() -> done.complete(null));
            }
            stats.up.addAndGet(b.length);
            byte[] plain = ctx.cltDec().update(b), cipher = ctx.tgEnc().update(plain);
            List<byte[]> parts = splitter == null ? List.of(cipher) : splitter.split(cipher, plain);
            return remote.sendBatch(parts).thenRun(() -> activity.sent(b.length));
        }).whenComplete((v, e) -> {
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                wsUp(client, remote, ctx, splitter, activity, done);
            }
        });
    }

    private void wsDown(ByteStream client, RawWebSocket remote, AesCipher.Context ctx, NetworkDebug activity,
            CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        remote.recv().thenCompose(b -> {
            if (b == null) {
                done.complete(null);
                return CompletableFuture.completedFuture(null);
            }
            stats.down.addAndGet(b.length);
            activity.received(b.length);
            return client.write(ctx.cltEnc().update(ctx.tgDec().update(b)));
        }).whenComplete((v, e) -> {
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                wsDown(client, remote, ctx, activity, done);
            }
        });
    }

    public static final class MsgSplitter {
        private final int proto;
        private byte[] cipher = new byte[0], plain = new byte[0];
        private boolean disabled;

        public MsgSplitter(int proto) {
            this.proto = proto;
        }

        public List<byte[]> split(byte[] c, byte[] p) {
            if (disabled) {
                return List.of(c);
            }
            cipher = Utils.concat(cipher, c);
            plain = Utils.concat(plain, p);
            List<byte[]> out = new ArrayList<>();
            int offset = 0;
            while (offset < cipher.length) {
                int available = cipher.length - offset, header = 4;
                long length;
                if (proto == Utils.ABRIDGED) {
                    int first = plain[offset] & 255;
                    if (first == 127 || first == 255) {
                        if (available < 4) {
                            break;
                        }
                        length = ((plain[offset + 1] & 255) | ((plain[offset + 2] & 255) << 8)
                                | ((plain[offset + 3] & 255) << 16)) * 4L;
                    } else {
                        header = 1;
                        length = (first & 127) * 4L;
                    }
                } else {
                    if (available < 4) {
                        break;
                    }
                    length = Utils.le(plain, offset) & 0x7fffffffL;
                }
                if (length <= 0) {
                    out.add(Utils.slice(cipher, offset, cipher.length));
                    offset = cipher.length;
                    disabled = true;
                    break;
                }
                if (length + header > available) {
                    break;
                }
                int n = (int) length + header;
                out.add(Utils.slice(cipher, offset, offset + n));
                offset += n;
            }
            cipher = Utils.slice(cipher, offset, cipher.length);
            plain = Utils.slice(plain, offset, plain.length);
            return out;
        }

        public byte[] flush() {
            byte[] b = cipher;
            cipher = new byte[0];
            plain = new byte[0];
            return b;
        }
    }

    private record NativePacket(byte[] body, boolean quick) {
    }

    private CompletableFuture<byte[]> decrypt(ByteStream client, AesCipher dec, int n) {
        return client.read(n).thenApply(dec::update);
    }

    private CompletableFuture<NativePacket> packet(ByteStream client, AesCipher dec, int tag) {
        CompletableFuture<byte[]> prefix = decrypt(client, dec, tag == Utils.ABRIDGED ? 1 : 4);
        return prefix.thenCompose(p -> {
            boolean quick = (p[tag == Utils.ABRIDGED ? 0 : 3] & 128) != 0;
            CompletableFuture<Integer> len;
            if (tag == Utils.ABRIDGED) {
                int words = p[0] & 127;
                len = words == 127
                        ? decrypt(client, dec, 3).thenApply(x ->
                                ((x[0] & 255) | ((x[1] & 255) << 8) | ((x[2] & 255) << 16)) * 4)
                        : CompletableFuture.completedFuture(words * 4);
            } else {
                len = CompletableFuture.completedFuture(Utils.le(p, 0) & 0x7fffffff);
            }
            return len.thenCompose(n -> {
                if (n < 24 || n > 4 * 1024 * 1024 + (tag == Utils.SECURE ? 256 : 0)) {
                    return Utils.failed(new IOException("Native packet length outside bound"));
                }
                return decrypt(client, dec, n).thenApply(body -> {
                    if (tag == Utils.SECURE) {
                        boolean zero = Arrays.equals(Utils.slice(body, 0, 8), new byte[8]);
                        long actual = zero ? 20L + Integer.toUnsignedLong(Utils.le(body, 16))
                                : 24L + (n - 24) / 16 * 16;
                        long pad = n - actual;
                        if (actual < 24 || actual > n || pad > (zero ? 256 : 15)) {
                            throw new CompletionException(new IOException("Invalid MTProto padding"));
                        }
                        body = Utils.slice(body, 0, (int) actual);
                    }
                    if (body.length % 4 != 0 || body.length > 4 * 1024 * 1024) {
                        throw new CompletionException(new IOException("Invalid MTProto packet alignment"));
                    }
                    return new NativePacket(body, quick);
                });
            });
        });
    }

    public static byte[] encode(byte[] body, int tag) {
        if (body.length == 0 || body.length % 4 != 0 || body.length > 4 * 1024 * 1024) {
            throw new IllegalArgumentException("Invalid reply packet");
        }
        if (tag == Utils.ABRIDGED) {
            int words = body.length / 4;
            return Utils.concat(words < 127 ? new byte[]{(byte) words}
                    : new byte[]{127, (byte) words, (byte) (words >>> 8), (byte) (words >>> 16)}, body);
        }
        byte[] pad = tag == Utils.SECURE ? Utils.random(Utils.RANDOM.nextInt(4)) : new byte[0];
        return Utils.concat(Utils.le(body.length + pad.length), body, pad);
    }

    private CompletableFuture<Void> h2Bridge(ByteStream client, CloudflareH2Client.HttpChannel channel,
            AesCipher.Context ctx, int tag) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        h2Up(client, channel, ctx, tag, done);
        h2Down(client, channel, ctx, tag, done);
        return done.handle((v, e) -> {
            Throwable cause = e;
            while (cause instanceof CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof CloudflareH2Client.TransportError te) {
                return client.write(ctx.cltEnc().update(encode(Utils.le(te.code), tag)));
            }
            return CompletableFuture.<Void>completedFuture(null);
        }).thenCompose(f -> f).whenComplete((v, e) -> {
            channel.close();
            client.close();
        });
    }

    private void h2Up(ByteStream client, CloudflareH2Client.HttpChannel channel, AesCipher.Context ctx,
            int tag, CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        packet(client, ctx.cltDec(), tag).thenCompose(p -> channel.send(p.body)).whenComplete((v, e) -> {
            if (e != null) {
                done.completeExceptionally(e);
            } else {
                h2Up(client, channel, ctx, tag, done);
            }
        });
    }

    private void h2Down(ByteStream client, CloudflareH2Client.HttpChannel channel, AesCipher.Context ctx,
            int tag, CompletableFuture<Void> done) {
        if (done.isDone()) {
            return;
        }
        channel.receive().thenCompose(body -> client.write(ctx.cltEnc().update(encode(body, tag)))
                .thenRun(() -> channel.delivered(body.length))).whenComplete((v, e) -> {
                    if (e != null) {
                        done.completeExceptionally(e);
                    } else {
                        h2Down(client, channel, ctx, tag, done);
                    }
                });
    }
}
