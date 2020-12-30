package org.apache.dubbo.rpc.protocol.tri;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

public class GrpcDecoder extends ReplayingDecoder<GrpcDecoder.GrpcDecodeState> {
    private int len;

    protected GrpcDecoder() {
        super(GrpcDecodeState.HEADER);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case HEADER:
                in.readByte();
                len = in.readInt();
                state(GrpcDecodeState.PAYLOAD);
            case PAYLOAD:
                ByteBuf buf = in.readSlice(len);
                out.add(buf);
                state(GrpcDecodeState.HEADER);
                break;
            default:
                throw new RuntimeException("Should not reach here");
        }
    }

    enum GrpcDecodeState {
        HEADER, PAYLOAD
    }
}
