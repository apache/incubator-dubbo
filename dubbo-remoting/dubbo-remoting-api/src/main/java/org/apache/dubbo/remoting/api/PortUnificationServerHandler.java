package org.apache.dubbo.remoting.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.List;

public class PortUnificationServerHandler extends ByteToMessageDecoder {

    private final SslContext sslCtx;
    private final boolean detectSsl;
    private final List<WireProtocol> protocols;
    private final DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public PortUnificationServerHandler(List<WireProtocol> protocols) {
        this(null, false, protocols);
    }

    public PortUnificationServerHandler(SslContext sslCtx, List<WireProtocol> protocols) {
        this(sslCtx, true, protocols);
    }

    public PortUnificationServerHandler(SslContext sslCtx, boolean detectSsl, List<WireProtocol> protocols) {
        this.sslCtx = sslCtx;
        this.protocols = protocols;
        this.detectSsl = detectSsl;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    public DefaultChannelGroup getChannels() {
        return channels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        channels.add(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        channels.remove(ctx.channel());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        if (isSsl(in)) {
            enableSsl(ctx);
        } else {
            for (final WireProtocol protocol : protocols) {
                in.markReaderIndex();
                final ProtocolDetector.Result result = protocol.detector().detect(ctx, in);
                in.resetReaderIndex();
                switch (result) {
                    case UNRECOGNIZED:
                        continue;
                    case RECOGNIZED:
                        protocol.configServerPipeline(ctx.pipeline(), null);
                        ctx.pipeline().remove(this);
                    case NEED_MORE_DATA:
                        return;
                }
            }
            // Unknown protocol; discard everything and close the connection.
            in.clear();
            ctx.close();
        }
    }

    private boolean isSsl(ByteBuf buf) {
        if (detectSsl) {
            return SslHandler.isEncrypted(buf);
        }
        return false;
    }

    private void enableSsl(ChannelHandlerContext ctx) {
        ChannelPipeline p = ctx.pipeline();
        p.addLast("ssl", sslCtx.newHandler(ctx.alloc()));
        p.addLast("unificationA", new PortUnificationServerHandler(sslCtx, false, protocols));
        p.remove(this);
    }

}
