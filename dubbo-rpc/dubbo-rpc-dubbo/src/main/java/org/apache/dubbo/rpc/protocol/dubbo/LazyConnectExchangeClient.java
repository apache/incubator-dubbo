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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Parameters;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Exchangers;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.remoting.Constants.SEND_RECONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_LAZY_CONNECT_INITIAL_STATE;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.LAZY_CONNECT_INITIAL_STATE_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.LAZY_CLOSE_STATE_KEY;

/**
 * dubbo protocol support class.
 */
@SuppressWarnings("deprecation")
final class LazyConnectExchangeClient implements ExchangeClient {

    /**
     * when this warning rises from invocation, program probably have bug.
     */
    protected static final String REQUEST_WITH_WARNING_KEY = "lazyclient_request_with_warning";
    private static final Logger logger = LoggerFactory.getLogger(LazyConnectExchangeClient.class);
    protected final boolean requestWithWarning;
    private final URL url;
    private final ExchangeHandler requestHandler;
    private final Lock connectLock = new ReentrantLock();
    private final int warning_period = 5000;
    /**
     * lazy connect, initial state for connection
     */
    private final boolean initialConnectState;
    private final Boolean closeState;
    private volatile ExchangeClient client;
    private AtomicLong warningcount = new AtomicLong(0);

    public LazyConnectExchangeClient(URL url, ExchangeHandler requestHandler) {
        // lazy connect, need set send.reconnect = true, to avoid channel bad status.
        this.url = url.addParameter(SEND_RECONNECT_KEY, Boolean.TRUE.toString());
        this.requestHandler = requestHandler;
        this.initialConnectState = url.getParameter(LAZY_CONNECT_INITIAL_STATE_KEY, DEFAULT_LAZY_CONNECT_INITIAL_STATE);
        if (url.hasParameter(LAZY_CLOSE_STATE_KEY)) {
            this.closeState = Boolean.parseBoolean(url.getParameter(LAZY_CLOSE_STATE_KEY));
        } else {
            this.closeState = null;
        }
        this.requestWithWarning = url.getParameter(REQUEST_WITH_WARNING_KEY, false);
    }

    private void initClient() throws RemotingException {
        if (client != null) {
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Lazy connect to " + url);
        }
        connectLock.lock();
        try {
            if (client != null) {
                return;
            }
            this.client = Exchangers.connect(url, requestHandler);
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        warning();
        initClient();
        return client.request(request);
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        if (client == null) {
            return InetSocketAddress.createUnresolved(url.getHost(), url.getPort());
        } else {
            return client.getRemoteAddress();
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        warning();
        initClient();
        return client.request(request, timeout);
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        warning();
        initClient();
        return client.request(request, executor);
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException {
        warning();
        initClient();
        return client.request(request, timeout, executor);
    }

    /**
     * If {@link #REQUEST_WITH_WARNING_KEY} is configured, then warn once every 5000 invocations.
     */
    private void warning() {
        if (requestWithWarning) {
            if (warningcount.get() % warning_period == 0) {
                logger.warn(url.getAddress() + " " + url.getServiceKey() + " safe guard client , should not be called ,must have a bug.");
            }
            warningcount.incrementAndGet();
        }
    }

    @Override
    public ChannelHandler getChannelHandler() {
        checkClient();
        return client.getChannelHandler();
    }

    @Override
    public boolean isConnected() {
        if (client == null) {
            return initialConnectState;
        } else {
            return client.isConnected();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        if (client == null) {
            return InetSocketAddress.createUnresolved(NetUtils.getLocalHost(), 0);
        } else {
            return client.getLocalAddress();
        }
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return requestHandler;
    }

    @Override
    public void send(Object message) throws RemotingException {
        initClient();
        client.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        initClient();
        client.send(message, sent);
    }

    @Override
    public boolean isClosed() {
        if (closeState != null) {
            return closeState;
        }

        if (client == null) {
            return false;
        } else {
            return client.isClosed();
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void close(int timeout) {
        if (client != null) {
            client.close(timeout);
        }
    }

    @Override
    public void startClose() {
        if (client != null) {
            client.startClose();
        }
    }

    @Override
    public void reset(URL url) {
        checkClient();
        client.reset(url);
    }

    @Override
    @Deprecated
    public void reset(Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void reconnect() throws RemotingException {
        checkClient();
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        if (client == null) {
            return null;
        } else {
            return client.getAttribute(key);
        }
    }

    @Override
    public void setAttribute(String key, Object value) {
        checkClient();
        client.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        checkClient();
        client.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        if (client == null) {
            return false;
        } else {
            return client.hasAttribute(key);
        }
    }

    private void checkClient() {
        if (client == null) {
            throw new IllegalStateException(
                    "LazyConnectExchangeClient state error. the client has not be init .url:" + url);
        }
    }
}
