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
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.remoting.Constants.SEND_RECONNECT_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.LAZY_CLOSE_STATE_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.LAZY_CONNECT_INITIAL_STATE_KEY;

/**
 * dubbo protocol support class.
 */
@SuppressWarnings("deprecation")
final class ReferenceCountExchangeClient implements ExchangeClient {

    private static final Logger logger = LoggerFactory.getLogger(ReferenceCountExchangeClient.class);
    private final URL url;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    private final AtomicInteger disconnectCount = new AtomicInteger(0);
    private final Integer maxDisconnectCount = 50;
    private ExchangeClient client;

    public ReferenceCountExchangeClient(ExchangeClient client) {
        this.client = client;
        referenceCount.incrementAndGet();
        this.url = client.getUrl();
    }

    @Override
    public void reset(URL url) {
        client.reset(url);
    }

    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return client.request(request);
    }

    @Override
    public URL getUrl() {
        return client.getUrl();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return client.getRemoteAddress();
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return client.getChannelHandler();
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return client.request(request, timeout);
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return client.request(request, executor);
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException {
        return client.request(request, timeout, executor);
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return client.getLocalAddress();
    }

    @Override
    public boolean hasAttribute(String key) {
        return client.hasAttribute(key);
    }

    @Override
    public void reset(Parameters parameters) {
        client.reset(parameters);
    }

    @Override
    public void send(Object message) throws RemotingException {
        client.send(message);
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return client.getExchangeHandler();
    }

    @Override
    public Object getAttribute(String key) {
        return client.getAttribute(key);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        client.send(message, sent);
    }

    @Override
    public void setAttribute(String key, Object value) {
        client.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        client.removeAttribute(key);
    }

    /**
     * close() is not idempotent any longer
     */
    @Override
    public void close() {
        close(0);
    }

    @Override
    public void close(int timeout) {
        if (referenceCount.decrementAndGet() <= 0) {
            if (timeout == 0) {
                client.close();

            } else {
                client.close(timeout);
            }

            replaceWithLazyClient();
        }
    }

    @Override
    public void startClose() {
        client.startClose();
    }

    /**
     * when closing the client, the client needs to be set to LazyConnectExchangeClient, and if a new call is made,
     * the client will "resurrect".
     *
     * @return
     */
    private void replaceWithLazyClient() {
        // this is a defensive operation to avoid client is closed by accident, the initial state of the client is false
        URL lazyUrl = url.addParameter(LAZY_CONNECT_INITIAL_STATE_KEY, Boolean.TRUE)
                .addParameter(LAZY_CLOSE_STATE_KEY, Boolean.TRUE)
                //.addParameter(RECONNECT_KEY, Boolean.FALSE)
                .addParameter(SEND_RECONNECT_KEY, Boolean.TRUE.toString());
        //.addParameter(LazyConnectExchangeClient.REQUEST_WITH_WARNING_KEY, true);

        if (disconnectCount.getAndIncrement() % maxDisconnectCount == 0) {
            logger.warn(url.getAddress() + " " + url.getServiceKey() + " safe guard client , should not be called ,must have a bug.");
        }

        /**
         * the order of judgment in the if statement cannot be changed.
         */
        if (!(client instanceof LazyConnectExchangeClient) || client.isClosed()) {
            client = new LazyConnectExchangeClient(lazyUrl, client.getExchangeHandler());
        }
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    /**
     * The reference count of current ExchangeClient, connection will be closed if all invokers destroyed.
     */
    public void incrementAndGetCount() {
        referenceCount.incrementAndGet();
    }

    public int getCount() {
        return referenceCount.get();
    }
}

