package org.apache.dubbo.rpc.protocol.tri;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class TripleInvokeServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        final UnaryInvoker invoker = TripleUtil.getInvoker(ctx);
        if (invoker != null) {
            invoker.receiveData(msg.retainedDuplicate());
        }
    }
}
