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

package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.handler.MockedChannel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultFutureTest {

    private static final AtomicInteger index = new AtomicInteger();

    @Test
    public void newFuture() {
        DefaultFuture future = defaultFuture(3000);
        Assertions.assertNotNull(future, "new future return null");
    }

    @Test
    public void isDone() {
        DefaultFuture future = defaultFuture(3000);
        Assertions.assertTrue(!future.isDone(), "init future is finished!");

        //cancel a future
        future.cancel();
        Assertions.assertTrue(future.isDone(), "cancel a future failed!");
    }

    /**
     * for example, it will print like this:
     * before a future is create , time is : 2018-06-21 15:06:17
     * after a future is timeout , time is : 2018-06-21 15:06:22
     * <p>
     * The exception info print like:
     * Sending request timeout in client-side by scan timer.
     * start time: 2018-06-21 15:13:02.215, end time: 2018-06-21 15:13:07.231...
     */
    @Test
    @Disabled
    public void timeoutNotSend() throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("before a future is create , time is : " + LocalDateTime.now().format(formatter));
        // timeout after 5 seconds.
        DefaultFuture f = defaultFuture(5000);
        while (!f.isDone()) {
            //spin
            Thread.sleep(100);
        }
        System.out.println("after a future is timeout , time is : " + LocalDateTime.now().format(formatter));

        // get operate will throw a timeout exception, because the future is timeout.
        try {
            f.get();
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof TimeoutException, "catch exception is not timeout exception!");
            System.out.println(e.getMessage());
        }
    }

    /**
     * for example, it will print like this:
     * before a future is create , time is : 2018-06-21 15:11:31
     * after a future is timeout , time is : 2018-06-21 15:11:36
     * <p>
     * The exception info print like:
     * Waiting server-side response timeout by scan timer.
     * start time: 2018-06-21 15:12:38.337, end time: 2018-06-21 15:12:43.354...
     */
    @Test
    @Disabled
    public void timeoutSend() throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("before a future is create , time is : " + LocalDateTime.now().format(formatter));
        // timeout after 5 seconds.
        Channel channel = new MockedChannel();
        Request request = new Request(10);
        DefaultFuture f = DefaultFuture.newFuture(channel, request, 5000, null);
        //mark the future is sent
        DefaultFuture.sent(channel, request);
        while (!f.isDone()) {
            //spin
            Thread.sleep(100);
        }
        System.out.println("after a future is timeout , time is : " + LocalDateTime.now().format(formatter));

        // get operate will throw a timeout exception, because the future is timeout.
        try {
            f.get();
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof TimeoutException, "catch exception is not timeout exception!");
            System.out.println(e.getMessage());
        }
    }
    /**
     * for example, it will print like this:
     *before a future is create , time is : 2021-01-22 10:55:03
     * null
     * after a future is timeout , time is : 2021-01-22 10:55:05
     */
    @Test
    public void interruptSend() throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println("before a future is create , time is : " + LocalDateTime.now().format(formatter));
        // timeout after 1 seconds.
        Channel channel = new MockedChannel();
        int channelId = 10;
        Request request = new Request(channelId);
        ExecutorService sharedExecutor = ExtensionLoader.getExtensionLoader(ExecutorRepository.class)
                .getDefaultExtension().createExecutorIfAbsent(URL.valueOf("dubbo://127.0.0.1:23456"));
        ThreadlessExecutor executor = new ThreadlessExecutor(sharedExecutor);
        DefaultFuture f = DefaultFuture.newFuture(channel, request, 1000, executor);
        //mark the future is sent
        DefaultFuture.sent(channel, request);
        // get operate will throw a interrupted exception, because the thread is interrupted.
        try {
            new InterruptThread(Thread.currentThread()).start();
            executor.waitAndDrain();
            f.get();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof InterruptedException, "catch exception is not interrupted exception!");
            System.out.println(e.getMessage());
        }
        //waiting timeout check task finished
        Thread.sleep(1500);
        System.out.println("after a future is timeout , time is : " + LocalDateTime.now().format(formatter));

        DefaultFuture future = DefaultFuture.getFuture(channelId);
        //waiting future should be removed by time out check task
        Assertions.assertNull(future);
    }

    @Test
    public void testClose() throws Exception {
        Channel channel = new MockedChannel();
        Request request = new Request(123);
        ExecutorService executor = ExtensionLoader.getExtensionLoader(ExecutorRepository.class)
                .getDefaultExtension().createExecutorIfAbsent(URL.valueOf("dubbo://127.0.0.1:23456"));
        DefaultFuture.newFuture(channel, request, 1000, executor);
        DefaultFuture.closeChannel(channel);
        Assertions.assertFalse(executor.isTerminated());
    }

    /**
     * mock a default future
     */
    private DefaultFuture defaultFuture(int timeout) {
        Channel channel = new MockedChannel();
        Request request = new Request(index.getAndIncrement());
        return DefaultFuture.newFuture(channel, request, timeout, null);
    }

    /**
     * mock a thread interrupt another thread which is waiting waitAndDrain() to return.
     */
    static class InterruptThread extends Thread {
        private Thread parent;

        public InterruptThread(Thread parent) {
            this.parent = parent;
        }

        @Override
        public void run() {
            super.run();
            try {
                //interrupt waiting thread before timeout
                Thread.sleep(500);
                parent.interrupt();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }

    }


}
