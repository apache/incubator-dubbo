package org.apache.dubbo.rpc.protocol.tri;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2GoAwayFrame;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import io.netty.util.concurrent.Future;

import java.util.concurrent.TimeUnit;

public class GracefulShutdown {
    static final long GRACEFUL_SHUTDOWN_PING = 0x97ACEF001L;
    private static final long GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE = Long.MAX_VALUE;
    private final ChannelHandlerContext ctx;
    private final ChannelPromise originPromise;
    boolean pingAckedOrTimeout;
    Long graceTimeInNanos;
    String goAwayMessage;
    Future<?> pingFuture;

    public GracefulShutdown(ChannelHandlerContext ctx, String goAwayMessage,
                            Long graceTimeInNanos, ChannelPromise originPromise) {
        this.ctx = ctx;
        this.goAwayMessage = goAwayMessage;
        this.graceTimeInNanos = graceTimeInNanos;
        this.originPromise = originPromise;
    }

    public void gracefulShutdown() {
        Http2GoAwayFrame goAwayFrame = new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR, ByteBufUtil
                .writeAscii(ctx.alloc(), goAwayMessage));
        goAwayFrame.setExtraStreamIds(Integer.MAX_VALUE);
        ctx.write(goAwayFrame);
        pingFuture = ctx.executor().schedule(
                () -> secondGoAwayAndClose(ctx),
                GRACEFUL_SHUTDOWN_PING_TIMEOUT_NANOS,
                TimeUnit.NANOSECONDS);

        Http2PingFrame pingFrame = new DefaultHttp2PingFrame(GRACEFUL_SHUTDOWN_PING, false);
        ctx.write(pingFrame);
    }

    void secondGoAwayAndClose(ChannelHandlerContext ctx) {
        if (pingAckedOrTimeout) {
            return;
        }
        pingAckedOrTimeout = true;

        //        checkNotNull(pingFuture, "pingFuture");
        pingFuture.cancel(false);

        try {
            Http2GoAwayFrame goAwayFrame = new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR, ByteBufUtil.writeAscii(this.ctx.alloc(), this.goAwayMessage));
            ctx.write(goAwayFrame);
            ctx.flush();
            //gracefulShutdownTimeoutMillis
            ctx.close(originPromise);
        } catch (Exception e) {
            ctx.fireExceptionCaught(e);
        }
    }
}
