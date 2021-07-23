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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.utils.UrlUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.IO_THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SSL_ENABLED_KEY;

/**
 * PortUnificationServer.
 */
public class PortUnificationServer {

    private static final Logger logger = LoggerFactory.getLogger(PortUnificationServer.class);
    private final List<WireProtocol> protocols;
    private final URL url;
    /**
     * netty server bootstrap.
     */
    private ServerBootstrap bootstrap;
    /**
     * the boss channel that receive connections and dispatch these to worker channel.
     */
    private Channel channel;
    private DefaultChannelGroup channelGroup;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public PortUnificationServer(URL url) {
        // you can customize name and type of client thread pool by THREAD_NAME_KEY and THREADPOOL_KEY in CommonConstants.
        // the handler will be wrapped: MultiMessageHandler->HeartbeatHandler->handler
        this.url = ExecutorUtil.setThreadName(url, "DubboPUServerHandler");
        this.protocols = ExtensionLoader.getExtensionLoader(WireProtocol.class).getActivateExtension(url, new String[0]);
    }

    public URL getUrl() {
        return url;
    }

    public void bind() {
        if (channel == null) {
            doOpen();
        }
    }

    public void close() throws Throwable {
        if (channel != null) {
            doClose();
        }
    }

    /**
     * Init and start netty server
     */
    protected void doOpen() {
        bootstrap = new ServerBootstrap();

        bossGroup = NettyEventLoopFactory.eventLoopGroup(1, "NettyServerBoss");
        workerGroup = NettyEventLoopFactory.eventLoopGroup(
            getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS),
            "NettyServerWorker");

        bootstrap.group(bossGroup, workerGroup)
            .channel(NettyEventLoopFactory.serverSocketChannelClass())
            .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
            .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    // FIXME: should we use getTimeout()?
                    int idleTimeout = UrlUtils.getIdleTimeout(getUrl());
                    final ChannelPipeline p = ch.pipeline();
//                        p.addLast(new LoggingHandler(LogLevel.DEBUG));

                    final boolean enableSsl = getUrl().getParameter(SSL_ENABLED_KEY, false);
                    if (enableSsl) {
                        p.addLast("negotiation-ssl", new SslServerTlsHandler(getUrl()));
                    }

                    final PortUnificationServerHandler puHandler = new PortUnificationServerHandler(protocols);
                    p.addLast("server-idle-handler", new IdleStateHandler(0, 0, idleTimeout, MILLISECONDS));
                    p.addLast("negotiation-protocol", puHandler);
                    channelGroup = puHandler.getChannels();
                }
            });
        // bind

        String bindIp = getUrl().getParameter(Constants.BIND_IP_KEY, getUrl().getHost());
        int bindPort = getUrl().getParameter(Constants.BIND_PORT_KEY, getUrl().getPort());
        if (url.getParameter(ANYHOST_KEY, false) || NetUtils.isInvalidLocalHost(bindIp)) {
            bindIp = ANYHOST_VALUE;
        }
        InetSocketAddress bindAddress = new InetSocketAddress(bindIp, bindPort);
        ChannelFuture channelFuture = bootstrap.bind(bindAddress);
        channelFuture.syncUninterruptibly();
        channel = channelFuture.channel();
    }

    protected void doClose() throws Throwable {
        final long st = System.currentTimeMillis();

        try {
            if (channel != null) {
                // unbind.
                channel.close();
                channel = null;
            }

            if (channelGroup != null) {
                ChannelGroupFuture closeFuture = channelGroup.close();
                closeFuture.await(15000);
            }
            final long cost = System.currentTimeMillis() - st;
            logger.info("Port unification server closed. cost:" + cost);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while shutting down", e);
        }

        for (WireProtocol protocol : protocols) {
            protocol.close();
        }

        try {
            if (bootstrap != null) {
                bossGroup.shutdownGracefully().syncUninterruptibly();
                workerGroup.shutdownGracefully().syncUninterruptibly();
            }
        } catch (Throwable e) {
            logger.warn(e.getMessage(), e);
        }
    }

    public boolean isBound() {
        return channel.isActive();
    }

    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

}
