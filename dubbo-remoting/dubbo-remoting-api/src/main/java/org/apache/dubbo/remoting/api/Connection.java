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

import io.netty.handler.ssl.SslContext;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.dubbo.common.constants.CommonConstants.SSL_ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADPOOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_CLIENT_THREADPOOL;

import static org.apache.dubbo.remoting.api.NettyEventLoopFactory.socketChannelClass;

public class Connection extends AbstractReferenceCounted implements ReferenceCounted {

    public static final AttributeKey<Connection> CONNECTION = AttributeKey.valueOf("connection");
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    private final URL url;
    private final int connectTimeout;
    private final WireProtocol protocol;
    private final Promise<Void> closeFuture;
    private final InetSocketAddress remote;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final ChannelFuture initPromise;
    private volatile CompletableFuture<Object> connectedFuture = new CompletableFuture<>();
    private static final Object CONNECTED_OBJECT = new Object();
    private final Bootstrap bootstrap;
    private final ConnectionListener connectionListener = new ConnectionListener();

    public Connection(URL url) {
        url = ExecutorUtil.setThreadName(url, "DubboClientHandler");
        url = url.addParameterIfAbsent(THREADPOOL_KEY, DEFAULT_CLIENT_THREADPOOL);
        this.url = url;
        this.protocol = ExtensionLoader.getExtensionLoader(WireProtocol.class).getExtension(url.getProtocol());
        this.connectTimeout = Math.max(3000, url.getPositiveParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT));
        this.closeFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
        this.remote = getConnectAddress();
        this.bootstrap = create();
        this.initPromise = connect();
    }

    public static Connection getConnectionFromChannel(Channel channel) {
        return channel.attr(CONNECTION).get();
    }

    public Promise<Void> getCloseFuture() {
        return closeFuture;
    }

    private Bootstrap create() {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(NettyEventLoopFactory.NIO_EVENT_LOOP_GROUP)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .remoteAddress(getConnectAddress())
            .channel(socketChannelClass());

        final ConnectionHandler connectionHandler = new ConnectionHandler(this);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                ch.attr(CONNECTION).set(Connection.this);

                SslContext sslContext = null;
                if (getUrl().getParameter(SSL_ENABLED_KEY, false)) {
                    ch.pipeline().addLast("negotiation", new SslClientTlsHandler(url));
                }

                final ChannelPipeline p = ch.pipeline();//.addLast("logging",new LoggingHandler(LogLevel.INFO))//for debug
                // TODO support IDLE
//                int heartbeatInterval = UrlUtils.getHeartbeat(getUrl());
//                p.addLast("client-idle-handler", new IdleStateHandler(heartbeatInterval, 0, 0, MILLISECONDS));
                p.addLast(connectionHandler);
                protocol.configClientPipeline(p, sslContext);
                // TODO support Socks5
            }
        });
        return bootstrap;
    }

    public ChannelFuture connect() {
        if (isClosed()) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("%s aborted to reconnect cause connection closed. ", Connection.this));
            }
            return null;
        }

        final ChannelFuture promise = bootstrap.connect();
        promise.addListener(this.connectionListener);
        return promise;
    }

    public Channel getChannel() {
        return channel.get();
    }

    @Override
    public String toString() {
        return super.toString() + " (Ref=" + ReferenceCountUtil.refCnt(this) + ",local=" +
            (getChannel() == null ? null : getChannel().localAddress()) + ",remote=" + getRemote();
    }

    public void onGoaway(Channel channel) {
        if (this.channel.compareAndSet(channel, null)) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("%s goaway", this));
            }
        }
        this.connectedFuture = new CompletableFuture<>();
    }

    public void onConnected(Channel channel) {
        this.channel.set(channel);
        // This indicates that the connection is available.
        this.connectedFuture.complete(CONNECTED_OBJECT);
        channel.attr(CONNECTION).set(this);
        if (logger.isInfoEnabled()) {
            logger.info(String.format("%s connected ", this));
        }
    }

    public void connectSync() throws InterruptedException, ExecutionException, TimeoutException {
        this.connectedFuture.get(this.connectTimeout, TimeUnit.MILLISECONDS);
    }

    public boolean isAvailable() {
        final Channel channel = getChannel();
        return channel != null && channel.isActive();
    }

    public boolean isClosed() {
        return closed.get();
    }

    //TODO replace channelFuture with intermediate future
    public ChannelFuture write(Object request) throws RemotingException {
        if (!isAvailable()) {
            throw new RemotingException(null, null,
                "Failed to send request " + request + ", cause: The channel to " + remote + " is closed!");
        }
        return getChannel().writeAndFlush(request);
    }

    public InetSocketAddress getRemote() {
        return remote;
    }

    @Override
    protected void deallocate() {
        if (closed.compareAndSet(false, true)) {
            close();
        }
        closeFuture.setSuccess(null);
    }

    public void close() {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("Connection:%s freed ", this));
        }
        final Channel current = this.channel.get();
        if (current != null) {
            current.close();
        }
        this.channel.set(null);
        this.connectedFuture = new CompletableFuture<>();
    }

    @Override
    public ReferenceCounted touch(Object hint) {
        return this;
    }

    private InetSocketAddress getConnectAddress() {
        return new InetSocketAddress(NetUtils.filterLocalHost(getUrl().getHost()), getUrl().getPort());
    }

    /**
     * get url.
     *
     * @return url
     */
    public URL getUrl() {
        return url;
    }

    class ConnectionListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) {
            if (future.isSuccess()) {
                return;
            }
            final Connection conn = Connection.this;
            if (conn.isClosed() || conn.refCnt() == 0) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("%s aborted to reconnect. %s", conn, future.cause().getMessage()));
                }
                return;
            }
            if (logger.isInfoEnabled()) {
                logger.info(String.format("%s is reconnecting, attempt=%d cause=%s", conn, 0, future.cause().getMessage()));
            }
            final EventLoop loop = future.channel().eventLoop();
            loop.schedule((Runnable) conn::connect, 1L, TimeUnit.SECONDS);
        }
    }
}

