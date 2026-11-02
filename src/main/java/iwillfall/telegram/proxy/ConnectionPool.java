package iwillfall.telegram.proxy;

import java.util.*;
import java.util.concurrent.*;

public final class ConnectionPool implements AutoCloseable {
    private record Key(int dc, boolean media, boolean test, boolean worker) {
    }

    private record Idle(RawWebSocket ws, double created) {
    }

    private final Config config;
    private final EventLoopManager loops;
    private final Stats stats;
    private final Map<Key, Deque<Idle>> idle = new HashMap<>();
    private final Set<Key> filling = new HashSet<>();
    private final Map<Key, Integer> failures = new HashMap<>();
    private final Map<Key, Double> retry = new HashMap<>();
    private volatile boolean frontFirst;
    private boolean closed;

    public ConnectionPool(Config c, EventLoopManager l, Stats s) {
        config = c;
        loops = l;
        stats = s;
        l.io.next().scheduleAtFixedRate(this::rotate, 5, 5, TimeUnit.SECONDS);
    }

    public synchronized RawWebSocket get(int dc, boolean media, boolean test) {
        if (config.poolSize <= 0 || !config.redirects.containsKey(dc)) {
            return null;
        }
        return take(new Key(dc, media, test, false));
    }

    public synchronized RawWebSocket worker(int dc) {
        return take(new Key(dc, false, false, true));
    }

    private RawWebSocket take(Key k) {
        Deque<Idle> q = idle.computeIfAbsent(k, x -> new ArrayDeque<>());
        while (!q.isEmpty()) {
            Idle i = q.removeFirst();
            if (!i.ws.alive() || Utils.now() - i.created >= (k.worker ? 100 : 120)) {
                i.ws.close();
                continue;
            }
            if (k.worker) {
                stats.cfHits.incrementAndGet();
            } else {
                stats.hits.incrementAndGet();
            }
            refill(k);
            return i.ws;
        }
        if (k.worker) {
            stats.cfMisses.incrementAndGet();
        } else {
            stats.misses.incrementAndGet();
            refill(k);
        }
        return null;
    }

    public synchronized void warmup() {
        for (int dc : config.redirects.keySet()) {
            for (boolean m : List.of(false, true)) {
                refill(new Key(dc, m, config.forceTestDc, false));
            }
        }
        if (!config.workerDomains.isEmpty()) {
            for (int dc : Utils.DC.keySet()) {
                if (!config.redirects.containsKey(dc)) {
                    refill(new Key(dc, false, false, true));
                }
            }
        }
    }

    private synchronized void rotate() {
        if (closed) {
            return;
        }
        for (Key k : new ArrayList<>(idle.keySet())) {
            Deque<Idle> q = idle.get(k);
            q.removeIf(i -> {
                boolean stale = !i.ws.alive() || Utils.now() - i.created >= (k.worker ? 100 : 120);
                if (stale) {
                    i.ws.close();
                }
                return stale;
            });
            if (!k.worker) {
                refill(k);
            }
        }
    }

    private synchronized void refill(Key k) {
        if (closed || filling.contains(k) || Utils.now() < retry.getOrDefault(k, 0.0)) {
            return;
        }
        Deque<Idle> q = idle.computeIfAbsent(k, x -> new ArrayDeque<>());
        int needed = (k.worker ? Math.min(1, config.poolSize) : config.poolSize) - q.size();
        if (needed <= 0) {
            return;
        }
        filling.add(k);
        List<CompletableFuture<RawWebSocket>> fs = new ArrayList<>();
        for (int i = 0; i < needed; i++) {
            CompletableFuture<RawWebSocket> f = k.worker ? connectWorker(k.dc) : connectDirect(k, 0, 0);
            fs.add(f);
            f.thenAccept(ws -> {
                synchronized (this) {
                    if (ws != null) {
                        if (closed) {
                            ws.close();
                        } else {
                            q.add(new Idle(ws, Utils.now()));
                        }
                    }
                }
            });
        }
        CompletableFuture.allOf(fs.toArray(CompletableFuture[]::new)).whenComplete((v, e) -> {
            synchronized (this) {
                filling.remove(k);
                if (q.isEmpty()) {
                    int n = failures.merge(k, 1, Integer::sum);
                    retry.put(k, Utils.now() + Math.min(3600, Math.pow(2, Math.min(n - 1, 12))));
                } else {
                    failures.remove(k);
                    retry.remove(k);
                }
            }
        });
    }

    private CompletableFuture<RawWebSocket> connectDirect(Key k, int domainIndex, int mode) {
        List<String> domains = Utils.wsDomains(k.dc, k.media);
        if (domainIndex >= domains.size()) {
            return CompletableFuture.completedFuture(null);
        }
        boolean front = mode == 0 ? frontFirst : !frontFirst;
        return RawWebSocket.connect(loops, config, config.redirects.get(k.dc), domains.get(domainIndex),
                k.test ? "/apiws_test" : "/apiws", front ? "sprinthost.ru" : null, true, front ? 7 : 8)
                .handle((ws, e) -> {
                    if (e == null) {
                        frontFirst = front;
                        if (front) {
                            stats.front.incrementAndGet();
                        }
                        return CompletableFuture.completedFuture(ws);
                    }
                    stats.wsErrors.incrementAndGet();
                    return connectDirect(k, mode == 1 ? domainIndex + 1 : domainIndex, mode == 1 ? 0 : 1);
                }).thenCompose(f -> f);
    }

    public CompletableFuture<RawWebSocket> connectWorker(int dc) {
        return connectWorker(dc, false);
    }

    public CompletableFuture<RawWebSocket> connectWorker(int dc, boolean test) {
        List<String> ds = new ArrayList<>(new LinkedHashSet<>(config.workerDomains));
        Collections.shuffle(ds);
        return workerTry(dc, test, ds, 0);
    }

    private CompletableFuture<RawWebSocket> workerTry(int dc, boolean test, List<String> ds, int i) {
        if (i >= ds.size()) {
            return CompletableFuture.completedFuture(null);
        }
        String d = ds.get(i);
        String path = "/apiws?dst=" + (test ? Utils.TEST : Utils.DC).getOrDefault(dc, "") + "&dc=" + dc;
        return RawWebSocket.connect(loops, config, d, d, path, null, !config.disableSecure, 8).handle((w, e) -> {
            if (e == null) {
                return CompletableFuture.completedFuture(w);
            }
            reportFailure(d, e);
            return workerTry(dc, test, ds, i + 1);
        }).thenCompose(f -> f);
    }

    public void reportFailure(String domain, Throwable error) {
        return;
    }

    public synchronized void close() {
        closed = true;
        for (Deque<Idle> q : idle.values()) {
            for (Idle i : q) {
                i.ws.close();
            }
        }
        idle.clear();
    }
}
