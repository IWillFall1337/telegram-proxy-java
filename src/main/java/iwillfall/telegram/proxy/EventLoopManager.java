package iwillfall.telegram.proxy;

import io.netty.channel.group.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.util.concurrent.GlobalEventExecutor;

public final class EventLoopManager implements AutoCloseable {
    public final NioEventLoopGroup boss = new NioEventLoopGroup(1), io = new NioEventLoopGroup();
    public final io.netty.resolver.dns.DnsAddressResolverGroup resolver =
            new io.netty.resolver.dns.DnsAddressResolverGroup(
                    io.netty.channel.socket.nio.NioDatagramChannel.class,
                    DefaultDnsServerAddressStreamProvider.INSTANCE);
    public final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public void close() {
        resolver.close();
        channels.close().awaitUninterruptibly();
        boss.shutdownGracefully();
        io.shutdownGracefully().awaitUninterruptibly();
    }
}
