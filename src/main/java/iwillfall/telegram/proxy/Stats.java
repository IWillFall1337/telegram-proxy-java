package iwillfall.telegram.proxy;

import java.util.concurrent.atomic.AtomicLong;

public final class Stats {
    public final AtomicLong total = new AtomicLong(),
            active = new AtomicLong(),
            ws = new AtomicLong(),
            h2 = new AtomicLong(),
            h2Tcp = new AtomicLong(),
            h2Requests = new AtomicLong(),
            h2Errors = new AtomicLong(),
            tcp = new AtomicLong(),
            cf = new AtomicLong(),
            front = new AtomicLong(),
            bad = new AtomicLong(),
            masked = new AtomicLong(),
            wsErrors = new AtomicLong(),
            up = new AtomicLong(),
            down = new AtomicLong(),
            hits = new AtomicLong(),
            misses = new AtomicLong(),
            cfHits = new AtomicLong(),
            cfMisses = new AtomicLong();

    public String summary() {
        return "total=" + total + " active=" + active + " ws=" + ws + " h2=" + h2 + " h2_tcp=" + h2Tcp
                + " h2_req=" + h2Requests + " h2_err=" + h2Errors + " tcp_fb=" + tcp + " cf=" + cf
                + " front=" + front + " bad=" + bad + " masked=" + masked + " err=" + wsErrors
                + " pool=" + hits + "/" + (hits.get() + misses.get())
                + " cf_pool=" + cfHits + "/" + (cfHits.get() + cfMisses.get())
                + " up=" + Utils.human(up.get()) + " down=" + Utils.human(down.get());
    }
}
