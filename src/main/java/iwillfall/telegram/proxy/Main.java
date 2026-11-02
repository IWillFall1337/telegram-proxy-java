package iwillfall.telegram.proxy;

import ch.qos.logback.classic.*;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.pattern.MessageConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.*;
import ch.qos.logback.core.rolling.*;
import ch.qos.logback.core.util.FileSize;
import java.util.*;
import java.util.regex.*;
import org.slf4j.LoggerFactory;

public final class Main {
    public static final class CliWindowPolicy extends FixedWindowRollingPolicy {
        @Override
        protected int getMaxWindowSize() {
            return Integer.MAX_VALUE;
        }
    }

    public static final class CensoredMessage extends MessageConverter {
        private static final Pattern DOMAIN = Pattern.compile(
                "(?<![\\w-])(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\\.)+[a-zA-Z]{2,}(?![\\w-])");

        public String convert(ILoggingEvent event) {
            Matcher m = DOMAIN.matcher(event.getFormattedMessage());
            StringBuilder out = new StringBuilder();
            while (m.find()) {
                String d = m.group(), lower = d.toLowerCase(Locale.ROOT);
                if (lower.equals("telegram.org") || lower.endsWith(".telegram.org") || lower.endsWith(".log")) {
                    m.appendReplacement(out, Matcher.quoteReplacement(d));
                    continue;
                }
                String[] p = d.split("\\.");
                for (int i = 0; i < p.length - 1; i++) {
                    p[i] = p[i].substring(0, p[i].length() / 2) + "*".repeat(p[i].length() - p[i].length() / 2);
                }
                m.appendReplacement(out, Matcher.quoteReplacement(String.join(".", p)));
            }
            m.appendTail(out);
            return out.toString();
        }
    }

    private static PatternLayoutEncoder encoder(LoggerContext context) {
        PatternLayoutEncoder e = new PatternLayoutEncoder();
        e.setContext(context);
        e.setPattern("%d{HH:mm:ss}  %-5level  %safeMsg%n");
        e.start();
        return e;
    }

    public static void logging(Config c) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();
        Map<String, String> rules = new HashMap<>();
        rules.put("safeMsg", CensoredMessage.class.getName());
        context.putObject("PATTERN_RULE_REGISTRY", rules);
        ch.qos.logback.classic.Logger root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(c.verbose ? Level.DEBUG : Level.INFO);
        ConsoleAppender<ILoggingEvent> console = new ConsoleAppender<>();
        console.setContext(context);
        console.setTarget("System.err");
        console.setEncoder(encoder(context));
        console.start();
        root.addAppender(console);
        context.getLogger("io.netty").setLevel(Level.WARN);
        if (c.logFile != null) {
            RollingFileAppender<ILoggingEvent> file = new RollingFileAppender<>();
            file.setContext(context);
            file.setFile(c.logFile);
            file.setEncoder(encoder(context));
            FixedWindowRollingPolicy policy = new CliWindowPolicy();
            policy.setContext(context);
            policy.setParent(file);
            policy.setFileNamePattern(c.logFile + ".%i");
            policy.setMinIndex(1);
            policy.setMaxIndex(Math.max(1, c.logBackups));
            policy.start();
            SizeBasedTriggeringPolicy<ILoggingEvent> size = new SizeBasedTriggeringPolicy<>();
            size.setContext(context);
            size.setMaxFileSize(new FileSize(Math.max(1, (long) (c.logMaxMb * 1024 * 1024))));
            size.start();
            file.setRollingPolicy(policy);
            file.setTriggeringPolicy(size);
            file.start();
            root.addAppender(file);
        }
    }

    public static void main(String[] args) {
        try {
            Config c = Config.parse(args);
            if (c.help) {
                System.out.println("Telegram MTProto WebSocket Bridge Proxy\n--port N --host HOST --secret HEX --dc-ip [DC:IP] (repeatable)\n-v, --verbose --log-file PATH --log-max-mb MB --log-backups N\n--buf-kb KB --pool-size N --cfproxy-domain DOMAIN (repeatable)\n--cfproxy-worker-domain DOMAIN (repeatable) --no-h2 --cfproxy-h2-media\n--no-cfproxy --no-secure --fake-tls-domain DOMAIN --force-test-dc --proxy-protocol\nDefaults: 127.0.0.1:1443, buffer 256 KB, pool 4, log 5 MB / 1 backup.\nA generated secret is not written to logs; supply --secret to configure the Telegram client.");
                return;
            }
            logging(c);
            TgWsProxy proxy = new TgWsProxy(c);
            Runtime.getRuntime().addShutdownHook(new Thread(proxy::close, "proxy-shutdown"));
            proxy.start();
            System.out.println("https://t.me/proxy?server=PROXY_HOST&port=PROXY_PORT&secret=PROXY_SECRET".replaceAll("PROXY_HOST", c.host).replaceAll("PROXY_PORT", String.valueOf(c.port)).replaceAll("PROXY_SECRET", c.secret));
            proxy.await();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LoggerFactory.getLogger(Main.class).error("Proxy startup failed", e);
            System.exit(1);
        }
    }
}
