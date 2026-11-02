package iwillfall.telegram.proxy;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.*;

public final class CloudflareH2Client implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CloudflareH2Client.class);
    private final EventLoopManager loops;
    private final Stats stats;
    private final Balancer balancer;
    private final io.netty.channel.EventLoop recoveryLoop;
    private final Map<String, Lane> lanes = new HashMap<>();
    private final Map<String, CompletableFuture<Lane>> settingUp = new HashMap<>();
    private final Map<String, Double> failed = new HashMap<>();
    private final Map<Integer, Double> dcRetry = new HashMap<>();
    private boolean closed;

    public CloudflareH2Client(EventLoopManager loops, Stats stats, Balancer balancer) {
        this.loops = loops;
        this.stats = stats;
        this.balancer = balancer;
        this.recoveryLoop = loops.io.next();
    }

    public CompletableFuture<HttpChannel> open(int dc) {
        synchronized (this) {
            if (closed || Utils.now() < dcRetry.getOrDefault(dc, 0.0) || !Utils.DC.containsKey(dc)) {
                return CompletableFuture.completedFuture(null);
            }
        }
        CompletableFuture<HttpChannel> f = tryDomain(dc, balancer.domains(dc), 0);
        CompletableFuture<HttpChannel> result = new CompletableFuture<>();
        f.whenComplete((v, e) -> {
            if (e != null) {
                result.complete(null);
            } else if (!result.complete(v) && v != null) {
                v.close();
            }
        });
        return result.completeOnTimeout(null, 8, TimeUnit.SECONDS).whenComplete((v, e) -> {
            if (v == null) {
                synchronized (this) {
                    dcRetry.put(dc, Utils.now() + 30);
                }
            }
        });
    }

    private CompletableFuture<HttpChannel> tryDomain(int dc, List<String> ds, int i) {
        if (i >= ds.size()) {
            return CompletableFuture.completedFuture(null);
        }
        String base = ds.get(i), host = "kws" + dc + "." + base;
        return lane(host).handle((l, e) -> {
            if (e != null || l == null) {
                return tryDomain(dc, ds, i + 1);
            }
            balancer.success(dc, base);
            LOG.info("ROUTE DC{} media -> H2 {}", dc, host);
            return CompletableFuture.completedFuture(new HttpChannel(l));
        }).thenCompose(f -> f);
    }

    private synchronized CompletableFuture<Lane> lane(String host) {
        if (closed || Utils.now() < failed.getOrDefault(host, 0.0)) {
            return CompletableFuture.completedFuture(null);
        }
        Lane l = lanes.get(host);
        if (l != null) {
            if (Utils.now() < l.failedUntil) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(l);
        }
        if (settingUp.containsKey(host)) {
            return settingUp.get(host);
        }
        Lane made = new Lane(host);
        CompletableFuture<Lane> ready = made.transport.request("HEAD", new byte[0], new H2Transport.Trace() {
        }).orTimeout(8, TimeUnit.SECONDS).thenApply(r -> {
            if (!((r.status() >= 200 && r.status() < 300) || r.status() == 405 || r.status() == 501)) {
                throw new CompletionException(new IOException("Preflight HTTP " + r.status()));
            }
            return made;
        });
        settingUp.put(host, ready);
        ready.whenComplete((v, e) -> {
            synchronized (this) {
                settingUp.remove(host);
                if (e != null || closed) {
                    made.transport.close();
                    failed.put(host, Utils.now() + 30);
                    stats.h2Errors.incrementAndGet();
                } else {
                    lanes.put(host, made);
                }
            }
        });
        return ready;
    }

    public static final class TransportError extends IOException {
        public final int code;

        TransportError(int code) {
            super("MTProto transport error " + code);
            this.code = code;
        }
    }

    private final class Lane {
        final H2Transport transport;
        final Set<HttpChannel> channels = new HashSet<>();
        int inflight, queuedBytes;
        volatile double failedUntil;
        final Deque<Reservation> waiting = new ArrayDeque<>();

        Lane(String host) {
            transport = new H2Transport(host, loops, stats);
        }

        synchronized boolean capacity(int n) {
            return inflight < 64 && queuedBytes + n <= 32 * 1024 * 1024;
        }

        synchronized CompletableFuture<Boolean> reserve(int n, boolean wait) {
            if (capacity(n)) {
                inflight++;
                queuedBytes += n;
                return CompletableFuture.completedFuture(true);
            }
            if (!wait) {
                return CompletableFuture.completedFuture(false);
            }
            Reservation r = new Reservation(n, new CompletableFuture<>());
            waiting.add(r);
            r.future.orTimeout(65, TimeUnit.SECONDS).whenComplete((v, e) -> {
                synchronized (this) {
                    waiting.remove(r);
                }
            });
            return r.future;
        }

        synchronized void release(int n) {
            inflight--;
            queuedBytes -= n;
            while (!waiting.isEmpty()) {
                Reservation r = waiting.peek();
                if (!capacity(r.bytes)) {
                    break;
                }
                waiting.remove();
                if (r.future.isDone()) {
                    continue;
                }
                inflight++;
                queuedBytes += r.bytes;
                loops.io.next().execute(() -> {
                    if (!r.future.complete(true)) {
                        release(r.bytes);
                    }
                });
            }
        }
    }

    private record Reservation(int bytes, CompletableFuture<Boolean> future) {
    }

    private static final class Packet {
        final double sent = Utils.now();
        final byte[] body;
        int attempts;
        double lastAttempt;
        Request replay;
        boolean retired;
        byte[] replyId;
        final Set<Request> originals = new HashSet<>();

        Packet(byte[] body) {
            this.body = body;
        }
    }

    private static final class Request {
        final Packet packet;
        final byte[] body;
        final boolean replay;
        final double created = Utils.now();
        double sent;
        boolean receiving;
        CompletableFuture<H2Transport.Response> future;

        Request(Packet p, byte[] b, boolean r) {
            packet = p;
            body = b;
            replay = r;
        }
    }

    public final class HttpChannel implements AutoCloseable {
        private final Lane lane;
        private final Set<Request> pending = new LinkedHashSet<>();
        private final List<Packet> history = new ArrayList<>();
        private byte[] key;
        private int pendingBytes, replyBytes;
        private final Deque<byte[]> replies = new ArrayDeque<>();
        private final Deque<Upload> uploads = new ArrayDeque<>();
        private CompletableFuture<byte[]> receiver;
        private Throwable error;
        private boolean closed, delivering;
        private double progress = Utils.now();
        private final io.netty.util.concurrent.ScheduledFuture<?> recovery;

        HttpChannel(Lane lane) {
            this.lane = lane;
            synchronized (lane) {
                lane.channels.add(this);
            }
            recovery = recoveryLoop.scheduleAtFixedRate(this::recover, 250, 250, TimeUnit.MILLISECONDS);
        }

        private boolean capacity(int n) {
            return pending.size() < 8 && pendingBytes + n <= 8 * 1024 * 1024;
        }

        public synchronized CompletableFuture<Void> send(byte[] body) {
            if (closed) {
                return Utils.failed(error != null ? error : new IOException("H2 channel closed"));
            }
            CompletableFuture<Void> f = new CompletableFuture<>();
            uploads.add(new Upload(body, f));
            f.orTimeout(65, TimeUnit.SECONDS);
            pump();
            return f;
        }

        private record Upload(byte[] body, CompletableFuture<Void> future) {
        }

        private void pump() {
            while (!uploads.isEmpty()) {
                Upload u = uploads.peek();
                if (u.future.isDone()) {
                    uploads.remove();
                    continue;
                }
                if (!capacity(u.body.length)) {
                    return;
                }
                uploads.remove();
                Packet p = remember(u.body);
                start(p, u.body, false).whenComplete((v, e) -> {
                    if (e != null) {
                        u.future.completeExceptionally(e);
                    } else if (Boolean.TRUE.equals(v)) {
                        u.future.complete(null);
                    } else {
                        u.future.completeExceptionally(error != null ? error : new IOException("H2 request not started"));
                    }
                });
            }
        }

        private Packet remember(byte[] body) {
            if (Arrays.equals(Utils.slice(body, 0, 8), new byte[8])) {
                return null;
            }
            byte[] k = Utils.slice(body, 0, 8);
            if (!Arrays.equals(k, key)) {
                history.clear();
                key = k;
            }
            if (body.length > 65536) {
                return null;
            }
            for (Packet p : history) {
                if (Arrays.equals(p.body, body)) {
                    return p;
                }
            }
            Packet p = new Packet(body);
            history.add(p);
            while (history.size() > 16 || history.stream().mapToInt(x -> x.body.length).sum() > 65536) {
                Packet victim = history.stream()
                        .filter(x -> x.originals.isEmpty() && !(x.retired && x.attempts < 3 && Utils.now() - x.sent <= 30))
                        .findFirst().orElse(history.get(0));
                history.remove(victim);
            }
            return p;
        }

        // Резерв канала учитывается до ожидания общего лимита lane; replay никогда не ждёт слот.
        private CompletableFuture<Boolean> start(Packet packet, byte[] body, boolean replay) {
            if (!capacity(body.length)) {
                return CompletableFuture.completedFuture(false);
            }
            Request req = new Request(packet, body, replay);
            pending.add(req);
            pendingBytes += body.length;
            if (packet != null) {
                if (replay) {
                    packet.replay = req;
                    packet.attempts++;
                    packet.lastAttempt = Utils.now();
                } else {
                    packet.originals.add(req);
                }
            }
            return lane.reserve(body.length, !replay).thenApply(reserved -> {
                synchronized (this) {
                    if (!reserved) {
                        finish(req, false);
                        return false;
                    }
                    if (closed) {
                        finish(req, true);
                        return false;
                    }
                    stats.h2Requests.incrementAndGet();
                    stats.up.addAndGet(body.length);
                    req.future = lane.transport.request("POST", body, new H2Transport.Trace() {
                        public void sent() {
                            synchronized (HttpChannel.this) {
                                req.sent = Utils.now();
                            }
                        }

                        public void headers() {
                            synchronized (HttpChannel.this) {
                                req.receiving = true;
                                progress = Utils.now();
                            }
                        }

                        public void data() {
                            synchronized (HttpChannel.this) {
                                progress = Utils.now();
                            }
                        }
                    });
                    req.future.whenComplete((r, e) -> {
                        synchronized (this) {
                            try {
                                if (closed) {
                                    return;
                                }
                                if (e != null) {
                                    if (!(e instanceof CancellationException)) {
                                        fail(e);
                                    }
                                    return;
                                }
                                byte[] reply = validate(r);
                                progress = Utils.now();
                                if (reply.length > 0) {
                                    if (packet != null && !Arrays.equals(packet.replyId,
                                            Utils.slice(reply, 0, Math.min(24, reply.length)))) {
                                        packet.replyId = Utils.slice(reply, 0, Math.min(24, reply.length));
                                        packet.attempts = 0;
                                    }
                                    if (replies.size() >= 16 || replyBytes + reply.length > 8 * 1024 * 1024) {
                                        throw new IOException("H2 reply queue exceeded bound");
                                    }
                                    replyBytes += reply.length;
                                    replies.add(reply);
                                    deliver();
                                }
                            } catch (TransportError ex) {
                                fail(ex);
                            } catch (Exception ex) {
                                lane.failedUntil = Utils.now() + 30;
                                fail(ex);
                            } finally {
                                finish(req, true);
                            }
                        }
                    });
                    return true;
                }
            }).exceptionally(e -> {
                synchronized (this) {
                    finish(req, false);
                    fail(e);
                }
                return false;
            });
        }

        private byte[] validate(H2Transport.Response r) throws IOException {
            int s = r.status();
            if (s == 403 || s == 404 || s == 429 || s == 444) {
                throw new TransportError(-s);
            }
            if (s != 200) {
                throw new IOException("CF /api HTTP " + s);
            }
            if (r.headers().getOrDefault("content-type", "").toLowerCase(Locale.ROOT).contains("text/html")) {
                throw new IOException("CF returned HTML");
            }
            byte[] b = r.body();
            if (b.length % 4 != 0) {
                throw new IOException("Unaligned MTProto reply");
            }
            if (b.length == 4 && Utils.le(b, 0) < 0) {
                throw new TransportError(Utils.le(b, 0));
            }
            return b;
        }

        private void finish(Request req, boolean reserved) {
            if (!pending.remove(req)) {
                return;
            }
            pendingBytes -= req.body.length;
            if (reserved) {
                lane.release(req.body.length);
            }
            if (req.packet != null) {
                req.packet.originals.remove(req);
                if (req.packet.replay == req) {
                    req.packet.replay = null;
                }
            }
            if (!closed) {
                pump();
            }
        }

        private void fail(Throwable e) {
            if (closed) {
                return;
            }
            stats.h2Errors.incrementAndGet();
            error = e;
            closed = true;
            LOG.warn("H2 channel failed: {}", e.toString());
            if (receiver != null) {
                receiver.completeExceptionally(e);
                receiver = null;
            }
            for (Upload u : uploads) {
                u.future.completeExceptionally(e);
            }
            uploads.clear();
            for (Request r : new ArrayList<>(pending)) {
                if (r.future != null) {
                    r.future.cancel(false);
                }
            }
            recovery.cancel(false);
        }

        public synchronized CompletableFuture<byte[]> receive() {
            if (closed) {
                return Utils.failed(error != null ? error : new IOException("H2 closed"));
            }
            if (receiver != null) {
                return Utils.failed(new IllegalStateException("Concurrent receiver"));
            }
            receiver = new CompletableFuture<>();
            CompletableFuture<byte[]> f = receiver;
            deliver();
            return f;
        }

        private void deliver() {
            if (receiver != null && !replies.isEmpty()) {
                byte[] b = replies.remove();
                replyBytes -= b.length;
                delivering = true;
                CompletableFuture<byte[]> f = receiver;
                receiver = null;
                f.complete(b);
            }
        }

        public synchronized void delivered(int bytes) {
            delivering = false;
            progress = Utils.now();
            stats.down.addAndGet(bytes);
        }

        private boolean overdue(Packet p, double now) {
            return p.retired || p.originals.stream().anyMatch(r -> r.sent > 0 && !r.receiving && now - r.sent >= 3);
        }

        private synchronized void recover() {
            if (closed) {
                return;
            }
            double now = Utils.now();
            if (!uploads.isEmpty()) {
                List<HttpChannel> cs = new ArrayList<>();
                synchronized (lane) {
                    cs.addAll(lane.channels);
                }
                for (HttpChannel c : cs) {
                    synchronized (c) {
                        if (uploads.isEmpty() || capacity(uploads.peek().body.length)
                                && lane.capacity(uploads.peek().body.length)) {
                            break;
                        }
                        Request victim = c.pending.stream()
                                .filter(r -> r.sent > 0 && !r.receiving && now - r.sent >= 3
                                        && (r.replay || r.packet != null && now - r.packet.sent <= 30))
                                .min(Comparator.comparingDouble(r -> r.replay ? 0 : r.sent)).orElse(null);
                        if (victim != null && victim.future != null) {
                            if (victim.packet != null) {
                                victim.packet.retired = true;
                            }
                            victim.future.cancel(false);
                            break;
                        }
                    }
                }
            }
            if (delivering || replyBytes > 0) {
                return;
            }
            List<Packet> candidates = new ArrayList<>(history);
            candidates.sort((a, b) -> {
                boolean ao = overdue(a, now), bo = overdue(b, now);
                return ao != bo ? (ao ? -1 : 1) : Double.compare(ao ? a.sent : -a.sent, bo ? b.sent : -b.sent);
            });
            for (Packet p : candidates) {
                boolean last = !history.isEmpty() && p == history.get(history.size() - 1);
                int limit = !p.originals.isEmpty() || p.retired || last ? 3 : 0;
                if (now - p.sent > 30 || p.attempts >= limit || p.originals.stream().anyMatch(r -> r.receiving)
                        || p.replay != null && p.replay.receiving || p.lastAttempt > 0 && now - p.lastAttempt < 2) {
                    continue;
                }
                boolean overdue = overdue(p, now);
                if (!overdue && (!last || pending.stream().anyMatch(r -> r != p.replay)
                        || p.lastAttempt > 0 && now - progress < 1)) {
                    continue;
                }
                Request replace = p.replay;
                if (replace == null && pending.stream().filter(r -> r.replay).count() >= 2) {
                    replace = pending.stream().filter(r -> r.replay && !r.receiving).findFirst().orElse(null);
                }
                if (replace != null) {
                    if (replace.receiving || replace.sent == 0 || now - replace.sent < 30) {
                        continue;
                    }
                    if (replace.future != null) {
                        replace.future.cancel(false);
                    }
                }
                if (capacity(p.body.length) && lane.capacity(p.body.length)) {
                    start(p, p.body, true);
                    break;
                }
            }
        }

        public synchronized void close() {
            if (!closed) {
                fail(new IOException("H2 channel closed"));
            }
            recovery.cancel(false);
            for (Request r : new ArrayList<>(pending)) {
                if (r.future != null) {
                    r.future.cancel(false);
                }
            }
            history.clear();
            replies.clear();
            synchronized (lane) {
                lane.channels.remove(this);
            }
        }
    }

    public synchronized void close() {
        closed = true;
        for (Lane l : lanes.values()) {
            for (HttpChannel c : new ArrayList<>(l.channels)) {
                c.close();
            }
            l.transport.close();
        }
        lanes.clear();
        for (CompletableFuture<Lane> f : new ArrayList<>(settingUp.values())) {
            f.cancel(false);
        }
        settingUp.clear();
    }
}
