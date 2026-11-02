package iwillfall.telegram.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.http2.*;
import io.netty.handler.ssl.*;
import io.netty.resolver.dns.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class H2Transport implements AutoCloseable {
    public record Response(int status, Map<String, String> headers, byte[] body) {
    }

    public interface Trace {
        default void sent() {
        }

        default void headers() {
        }

        default void data() {
        }
    }

    private final String host;
    private final EventLoopManager loops;
    private final Stats stats;
    private volatile Channel connection;
    private CompletableFuture<Channel> connecting;
    private boolean closed;
    private final AtomicLong replyBytes = new AtomicLong();

    public H2Transport(String host, EventLoopManager loops, Stats stats) {
        this.host = host;
        this.loops = loops;
        this.stats = stats;
    }

    private synchronized CompletableFuture<Channel> connection() {
        if (closed) {
            return Utils.failed(new IOException("H2 transport closed"));
        }
        if (connection != null && connection.isActive()) {
            return CompletableFuture.completedFuture(connection);
        }
        if (connecting != null) {
            return connecting;
        }
        CompletableFuture<Channel> f = new CompletableFuture<>();
        connecting = f;
        stats.h2Tcp.incrementAndGet();
        Bootstrap b = new Bootstrap().group(loops.io).channel(NioSocketChannel.class).resolver(loops.resolver)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 8000).option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel ch) throws Exception {
                        loops.channels.add(ch);
                        ch.pipeline().addLast(Utils.ssl(ch.alloc(), host, false, true));
                        ch.pipeline().addLast(new ApplicationProtocolNegotiationHandler("") {
                            protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
                                if (!"h2".equals(protocol)) {
                                    f.completeExceptionally(new IOException("Origin did not negotiate h2"));
                                    ctx.close();
                                    return;
                                }
                                Http2FrameCodec codec = Http2FrameCodecBuilder.forClient()
                                        .initialSettings(new Http2Settings().pushEnabled(false)
                                                .initialWindowSize(262144).maxConcurrentStreams(64))
                                        .build();
                                ctx.pipeline().addLast(codec);
                                ctx.pipeline().addLast(new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                                    protected void initChannel(Channel c) {
                                        c.close();
                                    }
                                }));
                                ctx.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                    public void channelRead(ChannelHandlerContext c, Object msg) {
                                        if (msg instanceof Http2GoAwayFrame) {
                                            io.netty.util.ReferenceCountUtil.release(msg);
                                            c.close();
                                        } else {
                                            io.netty.util.ReferenceCountUtil.release(msg);
                                        }
                                    }

                                    public void exceptionCaught(ChannelHandlerContext c, Throwable e) {
                                        c.close();
                                    }
                                });
                                codec.connection().local().flowController().incrementWindowSize(
                                        codec.connection().connectionStream(), 4 * 1024 * 1024 - 65535);
                                connection = ctx.channel();
                                if (!f.complete(connection)) {
                                    ctx.close();
                                }
                            }

                            protected void handshakeFailure(ChannelHandlerContext ctx, Throwable cause) {
                                f.completeExceptionally(cause);
                                ctx.close();
                            }
                        });
                    }
                });
        ChannelFuture dial = b.connect(host, 443);
        f.whenComplete((v, e) -> {
            if (e != null) {
                dial.channel().close();
            }
        });
        dial.addListener((ChannelFutureListener) r -> {
            if (!r.isSuccess()) {
                f.completeExceptionally(r.cause());
            }
        });
        f.orTimeout(8, TimeUnit.SECONDS).whenComplete((v, e) -> {
            synchronized (this) {
                connecting = null;
            }
            if (e != null && connection != null) {
                connection.close();
            }
        });
        return f;
    }

    public CompletableFuture<Response> request(String method, byte[] body, Trace trace) {
        CompletableFuture<Response> result = new CompletableFuture<>();
        connection().whenComplete((parent, err) -> {
            if (err != null) {
                result.completeExceptionally(err);
                return;
            }
            new Http2StreamChannelBootstrap(parent).handler(new SimpleChannelInboundHandler<Http2StreamFrame>() {
                private final ByteArrayOutputStream out = new ByteArrayOutputStream();
                private final Map<String, String> headers = new HashMap<>();
                private int status;
                private long allocated;
                private boolean ended;
                private Channel stream;

                public void handlerAdded(ChannelHandlerContext ctx) {
                    stream = ctx.channel();
                    result.whenComplete((v, e) -> {
                        if (e != null && stream.isActive()) {
                            stream.writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.CANCEL))
                                    .addListener(ChannelFutureListener.CLOSE);
                        }
                    });
                }

                protected void channelRead0(ChannelHandlerContext ctx, Http2StreamFrame frame) {
                    try {
                        if (frame instanceof Http2HeadersFrame h) {
                            for (Map.Entry<CharSequence, CharSequence> e : h.headers()) {
                                headers.put(e.getKey().toString(), e.getValue().toString());
                            }
                            if (h.headers().status() != null) {
                                int s = Integer.parseInt(h.headers().status().toString());
                                if (s >= 200) {
                                    status = s;
                                    trace.headers();
                                }
                            }
                            if (h.isEndStream()) {
                                finish(ctx);
                            }
                        } else if (frame instanceof Http2DataFrame d) {
                            int n = d.content().readableBytes();
                            long total = replyBytes.addAndGet(n);
                            allocated += n;
                            if (out.size() + n > 4 * 1024 * 1024 || total > 32 * 1024 * 1024) {
                                throw new IOException("H2 response buffer bound exceeded");
                            }
                            byte[] bytes = new byte[n];
                            d.content().readBytes(bytes);
                            out.writeBytes(bytes);
                            trace.data();
                            if (d.isEndStream()) {
                                finish(ctx);
                            }
                        } else if (frame instanceof Http2ResetFrame r) {
                            throw new IOException("H2 reset " + r.errorCode());
                        }
                    } catch (Exception e) {
                        result.completeExceptionally(e);
                        ctx.close();
                    }
                }

                private void finish(ChannelHandlerContext ctx) {
                    ended = true;
                    String length = headers.get("content-length");
                    if (!method.equals("HEAD") && length != null && Long.parseLong(length) != out.size()) {
                        result.completeExceptionally(new IOException("Incomplete H2 response"));
                    } else {
                        result.complete(new Response(status, Map.copyOf(headers), out.toByteArray()));
                    }
                    ctx.close();
                }

                public void channelInactive(ChannelHandlerContext ctx) {
                    if (!ended) {
                        result.completeExceptionally(new EOFException("H2 stream closed"));
                    }
                    replyBytes.addAndGet(-allocated);
                    allocated = 0;
                }

                public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
                    result.completeExceptionally(e);
                    ctx.close();
                }
            }).open().addListener(r -> {
                if (!r.isSuccess()) {
                    result.completeExceptionally(r.cause());
                    return;
                }
                Http2StreamChannel ch = (Http2StreamChannel) r.getNow();
                if (result.isDone()) {
                    ch.close();
                    return;
                }
                Http2Headers hs = new DefaultHttp2Headers().method(method).scheme("https").authority(host).path("/api");
                hs.set("accept-encoding", "identity");
                if (method.equals("POST")) {
                    hs.set("content-type", "application/octet-stream");
                    hs.setInt("content-length", body.length);
                }
                ChannelFuture sent;
                if (body.length == 0) {
                    sent = ch.writeAndFlush(new DefaultHttp2HeadersFrame(hs, true));
                } else {
                    ch.write(new DefaultHttp2HeadersFrame(hs, false));
                    sent = ch.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(body), true));
                }
                sent.addListener((ChannelFutureListener) s -> {
                    if (s.isSuccess()) {
                        trace.sent();
                    } else {
                        result.completeExceptionally(s.cause());
                    }
                });
            });
        });
        return result.orTimeout(65, TimeUnit.SECONDS);
    }

    public void close() {
        Channel channel;
        CompletableFuture<Channel> future;
        synchronized (this) {
            closed = true;
            channel = connection;
            future = connecting;
        }
        if (channel != null) {
            channel.close();
        }
        if (future != null) {
            future.cancel(false);
        }
    }
}
