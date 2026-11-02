package iwillfall.telegram.proxy;

import java.util.*;

public final class Balancer {
    private List<String> domains = List.of();
    private final Map<Integer, String> current = new HashMap<>();

    public synchronized void update(List<String> ds) {
        if (new HashSet<>(domains).equals(new HashSet<>(ds))) {
            return;
        }
        domains = List.copyOf(ds);
        current.clear();
        if (!domains.isEmpty()) {
            for (int dc : Utils.DC.keySet()) {
                current.put(dc, domains.get(Utils.RANDOM.nextInt(domains.size())));
            }
        }
    }

    public synchronized void success(int dc, String domain) {
        current.put(dc, domain);
    }

    public synchronized List<String> domains(int dc) {
        List<String> out = new ArrayList<>(domains);
        Collections.shuffle(out);
        String d = current.get(dc);
        if (d != null) {
            out.remove(d);
            out.add(0, d);
        }
        return out;
    }
}
