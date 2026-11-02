package iwillfall.telegram.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import java.io.*;
import java.util.concurrent.*;

public final class NettyStream extends ChannelInboundHandlerAdapter implements ByteStream {
    public Channel channel;
    private ByteBuf buffer = Unpooled.buffer();
    private CompletableFuture<byte[]> pending;
    private int wanted;
    private boolean eof;
    private Throwable failure;

    public static CompletableFuture<NettyStream> connect(EventLoopManager loops, Config config, String host,
            int port, String tlsName, boolean front) {
        CompletableFuture<NettyStream> result = new CompletableFuture<>();
        NettyStream stream = new NettyStream();
        Bootstrap b = new Bootstrap().group(loops.io).channel(NioSocketChannel.class).resolver(loops.resolver)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000).option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_RCVBUF, config.bufferSize).option(ChannelOption.SO_SNDBUF, config.bufferSize)
                .handler(new ChannelInitializer<Channel>() {
                    protected void initChannel(Channel ch) throws Exception {
                        loops.channels.add(ch);
                        if (tlsName != null) {
                            ch.pipeline().addLast(Utils.ssl(ch.alloc(), tlsName, front, false));
                        }
                        ch.pipeline().addLast(stream);
                    }
                });
        b.connect(host, port).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                result.completeExceptionally(f.cause());
                return;
            }
            SslHandler ssl = f.channel().pipeline().get(SslHandler.class);
            if (ssl == null) {
                result.complete(stream);
            } else {
                ssl.handshakeFuture().addListener(h -> {
                    if (h.isSuccess()) {
                        result.complete(stream);
                    } else {
                        f.channel().close();
                        result.completeExceptionally(h.cause());
                    }
                });
            }
        });
        result.whenComplete((v, e) -> {
            if (result.isCancelled()) {
                stream.close();
            }
        });
        return result;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        channel = ctx.channel();
    }

    // Exact-read может превышать high watermark: останавливаем чтение лишь после нужного объёма.
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf b = (ByteBuf) msg;
        try {
            if (buffer.readableBytes() + b.readableBytes() > 16 * 1024 * 1024) {
                exceptionCaught(ctx, new IOException("Input buffer exceeded bound"));
                return;
            }
            buffer.writeBytes(b);
            if (buffer.readableBytes() > Math.max(262144, wanted)) {
                channel.config().setAutoRead(false);
            }
            drain();
        } finally {
            b.release();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        eof = true;
        drain();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        failure = e;
        eof = true;
        drain();
        ctx.close();
    }

    private void drain() {
        if (pending == null) {
            return;
        }
        int n = wanted == 0 ? Math.min(65536, buffer.readableBytes()) : wanted;
        if (n > 0 && buffer.readableBytes() >= n) {
            byte[] b = new byte[n];
            buffer.readBytes(b);
            buffer.discardReadBytes();
            CompletableFuture<byte[]> p = pending;
            pending = null;
            if (buffer.readableBytes() < 131072) {
                channel.config().setAutoRead(true);
            }
            p.complete(b);
        } else if (eof) {
            CompletableFuture<byte[]> p = pending;
            pending = null;
            if (wanted == 0 && failure == null) {
                p.complete(new byte[0]);
            } else {
                p.completeExceptionally(failure != null ? failure : new EOFException());
            }
        }
    }

    public CompletableFuture<byte[]> read(int count) {
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        channel.eventLoop().execute(() -> {
            if (pending != null) {
                f.completeExceptionally(new IllegalStateException("Concurrent stream read"));
                return;
            }
            if (count < 0 || count > 16 * 1024 * 1024) {
                f.completeExceptionally(new IllegalArgumentException("Read outside buffer bound"));
                return;
            }
            if (buffer.refCnt() == 0) {
                f.completeExceptionally(new EOFException());
                return;
            }
            wanted = count;
            pending = f;
            if (!eof && buffer.readableBytes() < Math.max(262144, count)) {
                channel.config().setAutoRead(true);
            }
            drain();
        });
        return f;
    }

    public CompletableFuture<byte[]> line() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompletableFuture<byte[]> f = new CompletableFuture<>();
        lineNext(out, f);
        return f;
    }

    private void lineNext(ByteArrayOutputStream out, CompletableFuture<byte[]> f) {
        read(1).whenComplete((b, e) -> {
            if (e != null) {
                f.completeExceptionally(e);
                return;
            }
            out.write(b[0]);
            if (b[0] == 10) {
                f.complete(out.toByteArray());
            } else if (out.size() > 65536) {
                f.completeExceptionally(new IOException("Header line too long"));
            } else {
                lineNext(out, f);
            }
        });
    }

    public CompletableFuture<Void> write(byte[] bytes) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        channel.writeAndFlush(Unpooled.wrappedBuffer(bytes)).addListener((ChannelFutureListener) r -> {
            if (r.isSuccess()) {
                f.complete(null);
            } else {
                f.completeExceptionally(r.cause());
            }
        });
        return f;
    }

    public boolean alive() {
        return channel != null && channel.isActive() && !eof;
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        eof = true;
        if (pending != null) {
            CompletableFuture<byte[]> p = pending;
            pending = null;
            p.completeExceptionally(new EOFException());
        }
        buffer.release();
    }
}
