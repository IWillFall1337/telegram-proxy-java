package iwillfall.telegram.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.*;

public final class TgWsProxy implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TgWsProxy.class);
    private final Config config;
    private final EventLoopManager loops = new EventLoopManager();
    private final Stats stats = new Stats();
    private final Balancer balancer = new Balancer();
    private final ConnectionPool pool;
    private final CloudflareH2Client h2;
    private final Bridge bridge;
    private final byte[] secret;
    private volatile Channel server;
    private volatile boolean stopping;

    public TgWsProxy(Config config) {
        this.config = config;
        secret = HexFormat.of().parseHex(config.secret);
        balancer.update(config.userDomains.isEmpty() ? Config.defaults() : config.userDomains);
        pool = new ConnectionPool(config, loops, stats);
        h2 = config.h2Enabled() ? new CloudflareH2Client(loops, stats, balancer) : null;
        bridge = new Bridge(config, loops, stats, balancer, pool, h2);
    }

    private ChannelFuture bind() {
        return new ServerBootstrap().group(loops.boss, loops.io).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_RCVBUF, config.bufferSize)
                .childOption(ChannelOption.SO_SNDBUF, config.bufferSize)
                .childHandler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel ch) {
                        loops.channels.add(ch);
                        NettyStream s = new NettyStream();
                        ch.pipeline().addLast(s);
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                            public void channelActive(ChannelHandlerContext ctx) {
                                handle(s);
                                ctx.fireChannelActive();
                            }
                        });
                    }
                }).bind(config.host, config.port);
    }

    public void start() throws InterruptedException {
        server = bind().sync().channel();
        LOG.info("Telegram MTProto WS Bridge Proxy listening on {}:{}:{}; H2 media={}", config.host, config.port, config.secret, h2 != null);
        pool.warmup();
        loops.io.next().scheduleAtFixedRate(() -> LOG.info("stats: {}", stats.summary()), 60, 60, TimeUnit.SECONDS);
        loops.io.next().scheduleAtFixedRate(NetworkDebug::flow, 5, 5, TimeUnit.SECONDS);
        if (config.userDomains.isEmpty()) {
            config.refresh(balancer);
            loops.io.next().scheduleAtFixedRate(() -> config.refresh(balancer), 3600, 3600, TimeUnit.SECONDS);
        }
        loops.boss.next().scheduleAtFixedRate(() -> {
            if (!stopping && !server.isActive()) {
                LOG.warn("Listening socket lost; restarting");
                bind().addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        server = f.channel();
                    } else {
                        LOG.error("Listener restart failed", f.cause());
                    }
                });
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void handle(NettyStream stream) {
        stats.total.incrementAndGet();
        stats.active.incrementAndGet();
        CompletableFuture<Void> pp = config.proxyProtocol
                ? stream.line().orTimeout(10, TimeUnit.SECONDS).thenAccept(line -> {
                    String text = new String(line, StandardCharsets.US_ASCII).trim();
                    String[] parts = text.split("\\s+");
                    if (text.startsWith("PROXY ") && parts.length >= 6) {
                        LOG.debug("PROXY client {}:{}", parts[2], parts[4]);
                    } else {
                        LOG.debug("Expected PROXY header");
                    }
                }) : CompletableFuture.completedFuture(null);
        pp.thenCompose(v -> initial(stream)).whenComplete((v, e) -> {
            stats.active.decrementAndGet();
            if (e != null) {
                LOG.debug("Client {} closed: {}", stream.channel.remoteAddress(), e.toString());
            }
            stream.close();
        });
    }

    private CompletableFuture<Void> initial(NettyStream raw) {
        return raw.read(1).orTimeout(10, TimeUnit.SECONDS).thenCompose(first -> {
            if (config.fakeTlsDomain.isEmpty()) {
                return raw.read(63).orTimeout(10, TimeUnit.SECONDS)
                        .thenCompose(rest -> handshake(raw, Utils.concat(first, rest), raw.channel.remoteAddress().toString()));
            }
            if (first[0] != 22) {
                String response = "HTTP/1.1 301 Moved Permanently\r\nLocation: https://" + config.fakeTlsDomain
                        + "/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                return raw.write(response.getBytes(StandardCharsets.US_ASCII));
            }
            return raw.read(4).orTimeout(10, TimeUnit.SECONDS).thenCompose(rest -> {
                byte[] hdr = Utils.concat(first, rest);
                int n = ((hdr[3] & 255) << 8) | (hdr[4] & 255);
                if (n == 0) {
                    return Utils.failed(new IOException("Empty TLS ClientHello record"));
                }
                return raw.read(n).orTimeout(10, TimeUnit.SECONDS).thenCompose(body -> {
                    byte[] hello = Utils.concat(hdr, body), random = FakeTlsHandler.verify(hello, secret);
                    if (random == null) {
                        return NettyStream.connect(loops, config, config.fakeTlsDomain, 443, null, false)
                                .thenCompose(remote -> {
                                    stats.masked.incrementAndGet();
                                    return remote.write(hello).thenCompose(v -> Bridge.relay(raw, remote));
                                });
                    }
                    FakeTlsHandler tls = new FakeTlsHandler(raw);
                    return raw.write(FakeTlsHandler.serverHello(hello, secret, random))
                            .thenCompose(v -> tls.read(64).orTimeout(10, TimeUnit.SECONDS))
                            .thenCompose(init -> handshake(tls, init, raw.channel.remoteAddress().toString()));
                });
            });
        });
    }

    private CompletableFuture<Void> handshake(ByteStream client, byte[] init, String label) {
        AesCipher.Handshake hs = AesCipher.handshake(init, secret);
        if (hs == null) {
            stats.bad.incrementAndGet();
            return discard(client);
        }
        LOG.debug("Handshake DC{} media={} proto={}", hs.dc(), hs.media(), Integer.toHexString(hs.protocol()));
        return bridge.route(client, hs, secret, label);
    }

    private CompletableFuture<Void> discard(ByteStream s) {
        return s.read(0).thenCompose(b -> b.length == 0 ? CompletableFuture.completedFuture(null) : discard(s));
    }

    public void await() throws InterruptedException {
        while (!stopping) {
            Thread.sleep(1000);
        }
    }

    public void close() {
        if (stopping) {
            return;
        }
        stopping = true;
        if (server != null) {
            server.close();
        }
        pool.close();
        if (h2 != null) {
            h2.close();
        }
        loops.close();
        LOG.info("Shutting down. Final stats: {}", stats.summary());
    }
}
