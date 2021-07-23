/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.api;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import javax.net.ssl.SSLSession;
import java.util.List;

public class SslServerTlsHandler extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(SslServerTlsHandler.class);

    private final SslContext sslContext;
    private final boolean detectSsl;


    public SslServerTlsHandler() {
        this(null, false);
    }

    public SslServerTlsHandler(URL url) {
        this(SslContexts.buildServerSslContext(url));
    }

    public SslServerTlsHandler(SslContext sslContext) {
        this(sslContext, true);
    }

    public SslServerTlsHandler(SslContext sslContext, boolean detectSsl) {
        this.sslContext = sslContext;
        this.detectSsl = detectSsl;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("TLS negotiation failed when trying to accept new connection.", cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent) {
            SslHandshakeCompletionEvent handshakeEvent = (SslHandshakeCompletionEvent) evt;
            if (handshakeEvent.isSuccess()) {
                SSLSession session = ctx.pipeline().get(SslHandler.class).engine().getSession();
                logger.info("TLS negotiation succeed with session: " + session);
                // Remove after handshake success.
                ctx.pipeline().remove(this);
            } else {
                logger.error("TLS negotiation failed when trying to accept new connection.", handshakeEvent.cause());
                ctx.close();
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        // Will use the first five bytes to detect a protocol.
        if (byteBuf.readableBytes() < 5) {
            return;
        }

        if (isSsl(byteBuf)) {
            enableSsl(channelHandlerContext);
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
        ctx.pipeline().addAfter(ctx.name(), null, sslContext.newHandler(ctx.alloc()));
        p.addLast("unificationA", new SslServerTlsHandler(sslContext, false));
        p.remove(this);
    }

}
