package iwillfall.telegram.proxy;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.*;

public final class NetworkDebug implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkDebug.class);
    private static final Set<NetworkDebug> ACTIVE = ConcurrentHashMap.newKeySet();
    private final String label, dc;
    private final double started = Utils.now();
    private long nativeUp, wsUp, wsDown, nativeDown;
    private double firstInput = -1, firstReply = -1, lastRx = -1, lastTx = -1, awaitingRx = -1,
            sending = -1, writingNative = -1, maxRxGap, maxSend, maxNativeWrite;
    private double sampleTime = started;
    private long sampleNativeUp, sampleWsUp, sampleWsDown, sampleNativeDown;
    private boolean closed;

    public NetworkDebug(String label, String dc) {
        this.label = label;
        this.dc = dc;
        if (LOG.isDebugEnabled()) {
            ACTIVE.add(this);
        }
        LOG.debug("[{}] WS OPEN {}", label, dc);
    }

    public synchronized void input(int n) {
        nativeUp += n;
        if (firstInput < 0) {
            firstInput = Utils.now();
        }
    }

    public synchronized void sendStarted() {
        sending = Utils.now();
    }

    public synchronized void sent(int n) {
        double now = Utils.now();
        wsUp += n;
        if (sending >= 0) {
            maxSend = Math.max(maxSend, now - sending);
            if (awaitingRx < 0 && (lastRx < 0 || lastRx < sending)) {
                awaitingRx = sending;
            }
        }
        lastTx = now;
        sending = -1;
    }

    public synchronized void received(int n) {
        double now = Utils.now();
        wsDown += n;
        lastRx = now;
        if (awaitingRx >= 0) {
            maxRxGap = Math.max(maxRxGap, now - awaitingRx);
        }
        if (firstReply < 0 && firstInput >= 0) {
            firstReply = now - firstInput;
        }
        awaitingRx = -1;
        writingNative = now;
    }

    public synchronized void delivered(int n) {
        nativeDown += n;
        if (writingNative >= 0) {
            maxNativeWrite = Math.max(maxNativeWrite, Utils.now() - writingNative);
        }
        writingNative = -1;
    }

    private synchronized String summary(double now) {
        String s = String.format(Locale.ROOT,
                "client=%s window_ms=%.0f native_up=%d ws_up=%d ws_down=%d native_down=%d rx_gap_ms=%.0f last_rx_ms=%.0f ws_write_ms=%.0f native_write_ms=%.0f",
                label, (now - sampleTime) * 1000, nativeUp - sampleNativeUp, wsUp - sampleWsUp,
                wsDown - sampleWsDown, nativeDown - sampleNativeDown,
                awaitingRx < 0 ? 0 : (now - awaitingRx) * 1000,
                lastRx < 0 ? -1 : (now - lastRx) * 1000,
                sending < 0 ? 0 : (now - sending) * 1000,
                writingNative < 0 ? 0 : (now - writingNative) * 1000);
        sampleTime = now;
        sampleNativeUp = nativeUp;
        sampleWsUp = wsUp;
        sampleWsDown = wsDown;
        sampleNativeDown = nativeDown;
        return s;
    }

    public static void flow() {
        if (!LOG.isDebugEnabled()) {
            return;
        }
        double now = Utils.now();
        Map<String, List<NetworkDebug>> groups = new TreeMap<>();
        for (NetworkDebug a : ACTIVE) {
            groups.computeIfAbsent(a.dc, k -> new ArrayList<>()).add(a);
        }
        groups.forEach((dc, list) -> {
            list.sort(Comparator.comparing(a -> a.label));
            for (int i = 0; i < list.size(); i += 8) {
                List<String> summaries = new ArrayList<>();
                for (NetworkDebug a : list.subList(i, Math.min(i + 8, list.size()))) {
                    summaries.add(a.summary(now));
                }
                LOG.debug("WS FLOW {} [{}]", dc, String.join("; ", summaries));
            }
        });
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        ACTIVE.remove(this);
        LOG.debug("WS END {} {} first_reply_ms={} max_rx_gap_ms={} max_ws_write_ms={} max_native_write_ms={}",
                dc, summary(Utils.now()), firstReply < 0 ? -1 : firstReply * 1000,
                maxRxGap * 1000, maxSend * 1000, maxNativeWrite * 1000);
    }
}
