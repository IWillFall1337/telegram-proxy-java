package iwillfall.telegram.proxy;

import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

public final class Config {
    public int port = 59921, bufferSize = 262144, poolSize = 4, logBackups = 1;
    public String host = "127.0.0.1", secret = HexFormat.of().formatHex(Utils.random(16)), fakeTlsDomain = "", logFile;
    public double logMaxMb = 5;
    public boolean fallbackCfproxy = true, h2Media = true, disableSecure, proxyProtocol, forceTestDc, verbose, help;
    public final Map<Integer, String> redirects = new LinkedHashMap<>(Map.of(2, "149.154.167.220", 4, "149.154.167.220"));
    public final List<String> userDomains = new ArrayList<>(), workerDomains = new ArrayList<>();

    public boolean h2Enabled() {
        return h2Media && fallbackCfproxy && !disableSecure && !forceTestDc;
    }

    public static List<String> domains(String s) {
        Map<String, String> seen = new LinkedHashMap<>();
        for (String p : s.split("[,;\\s]+")) {
            if (!p.isBlank()) {
                seen.putIfAbsent(p.toLowerCase(Locale.ROOT), p);
            }
        }
        return new ArrayList<>(seen.values());
    }

    public static String decode(String s) {
        if (!s.endsWith(".com")) {
            return s;
        }
        String p = s.substring(0, s.length() - 4);
        int n = (int) p.chars().filter(Character::isLetter).count();
        StringBuilder b = new StringBuilder();
        for (char c : p.toCharArray()) {
            b.append(Character.isLetter(c)
                    ? (char) (Math.floorMod(c - (c > '`' ? 'a' : 'A') - n, 26) + (c > '`' ? 'a' : 'A')) : c);
        }
        return b + ".co.uk";
    }

    public static List<String> defaults() {
        return Arrays.stream("virkgj.com vmmzovy.com mkuosckvso.com zaewayzmplad.com twdmbzcm.com awzwsldi.com clngqrflngqin.com tjacxbqtj.com bxaxtxmrw.com dmohrsgmohcrwb.com vwbmtmoi.com khgrre.com ulihssf.com tmhqsdqmfpmk.com xwuwoqbm.com orgcnunpj.com zhkuldz.com zypoljnslxa.com efabnxaowuzs.com zaftuzsftqdq.com".split(" "))
                .map(Config::decode).toList();
    }

    public void refresh(Balancer balancer) {
        if (!userDomains.isEmpty()) {
            return;
        }
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
                .sendAsync(HttpRequest.newBuilder(URI.create(
                        "https://raw.githubusercontent.com/Flowseal/tg-ws-proxy/main/.github/cfproxy-domains.txt?"
                                + HexFormat.of().formatHex(Utils.random(4))))
                        .timeout(Duration.ofSeconds(10)).header("User-Agent", "tg-ws-proxy").build(),
                        HttpResponse.BodyHandlers.ofString())
                .thenAccept(r -> {
                    if (r.statusCode() != 200) {
                        return;
                    }
                    List<String> ds = r.body().lines().map(String::trim)
                            .filter(s -> !s.isEmpty() && !s.startsWith("#"))
                            .map(Config::decode).map(s -> s.toLowerCase(Locale.ROOT))
                            .filter(Config::validDomain).distinct().toList();
                    if (ds.size() >= 3) {
                        balancer.update(ds);
                    }
                }).exceptionally(e -> {
                    org.slf4j.LoggerFactory.getLogger(Config.class).warn("CF domain refresh failed: {}", e.toString());
                    return null;
                });
    }

    public static boolean validDomain(String s) {
        if (s.length() > 253 || !s.contains(".")) {
            return false;
        }
        String[] a = s.split("\\.", -1);
        for (String l : a) {
            if (!l.matches("[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?")) {
                return false;
            }
        }
        return a[a.length - 1].matches("[a-zA-Z]{2,}");
    }

    public static Config parse(String[] args) {
        Config c = new Config();
        if (Arrays.asList(args).contains("--help") || Arrays.asList(args).contains("-h")) {
            c.help = true;
            return c;
        }
        boolean dcSeen = false, clear = false, noH2 = false, yesH2 = false;
        for (int i = 0; i < args.length; i++) {
            String a = args[i], v = null;
            int eq = a.indexOf('=');
            if (eq >= 0) {
                v = a.substring(eq + 1);
                a = a.substring(0, eq);
            }
            Set<String> flags = Set.of("--help", "-h", "-v", "--verbose", "--no-h2", "--cfproxy-h2-media",
                    "--no-cfproxy", "--no-secure", "--force-test-dc", "--proxy-protocol");
            if (!flags.contains(a) && !a.equals("--dc-ip") && v == null) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value: " + a);
                }
                v = args[++i];
            }
            switch (a) {
                case "--help", "-h" -> c.help = true;
                case "--port" -> c.port = Integer.parseInt(v);
                case "--host" -> c.host = v;
                case "--secret" -> c.secret = v.trim();
                case "--buf-kb" -> c.bufferSize = Math.max(4, Integer.parseInt(v)) * 1024;
                case "--pool-size" -> c.poolSize = Math.max(0, Integer.parseInt(v));
                case "--log-file" -> c.logFile = v;
                case "--log-max-mb" -> c.logMaxMb = Double.parseDouble(v);
                case "--log-backups" -> c.logBackups = Math.max(1, Integer.parseInt(v));
                case "--cfproxy-domain" -> c.userDomains.addAll(domains(v));
                case "--cfproxy-worker-domain" -> c.workerDomains.addAll(domains(v));
                case "--fake-tls-domain" -> c.fakeTlsDomain = v.trim();
                case "-v", "--verbose" -> c.verbose = true;
                case "--no-h2" -> {
                    c.h2Media = false;
                    noH2 = true;
                }
                case "--cfproxy-h2-media" -> yesH2 = true;
                case "--no-cfproxy" -> c.fallbackCfproxy = false;
                case "--no-secure" -> c.disableSecure = true;
                case "--force-test-dc" -> c.forceTestDc = true;
                case "--proxy-protocol" -> c.proxyProtocol = true;
                case "--dc-ip" -> {
                    if (!dcSeen) {
                        c.redirects.clear();
                        dcSeen = true;
                    }
                    if (v == null && i + 1 < args.length && !args[i + 1].startsWith("-")) {
                        v = args[++i];
                    }
                    if (v == null) {
                        clear = true;
                        break;
                    }
                    String[] p = v.split(":", 2);
                    if (p.length != 2 || !p[1].matches("(?:[0-9]{1,3}\\.){3}[0-9]{1,3}")) {
                        throw new IllegalArgumentException("Invalid --dc-ip " + v);
                    }
                    for (String oct : p[1].split("\\.")) {
                        if (Integer.parseInt(oct) > 255) {
                            throw new IllegalArgumentException("Invalid IPv4");
                        }
                    }
                    c.redirects.put(Integer.parseInt(p[0]), p[1]);
                }
                default -> throw new IllegalArgumentException("Unknown argument " + a);
            }
        }
        if (clear) {
            c.redirects.clear();
        }
        if (noH2 && yesH2) {
            throw new IllegalArgumentException("Mutually exclusive H2 flags");
        }
        if (!c.secret.matches("[0-9a-fA-F]{32}")) {
            throw new IllegalArgumentException("Secret must be 32 hexadecimal characters");
        }
        if (c.port < 0 || c.port > 65535 || !Double.isFinite(c.logMaxMb) || c.logMaxMb <= 0) {
            throw new IllegalArgumentException("Invalid port or log size");
        }
        return c;
    }
}
