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
package org.apache.dubbo.rpc.protocol.http;

import org.apache.dubbo.rpc.RpcContext;

import java.util.concurrent.CompletableFuture;

public class HttpServiceImpl implements HttpService {
    private volatile boolean called;

    public String sayHello(String name) {
        called = true;
        return "Hello, " + name;
    }

    @Override
    public String blockingSayHello(String name) {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        called = true;
        return "Hello, " + name;
    }

    @Override
    public CompletableFuture<String> sayHelloAsync(String name) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            called = true;
            return "Hello, " + name;
        });
    }

    public boolean isCalled() {
        return called;
    }

    public void timeOut(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String customException() {
        throw new MyException("custom exception");
    }

    static class MyException extends RuntimeException{

        private static final long serialVersionUID = -3051041116483629056L;

        public MyException(String message) {
            super(message);
        }
    }

    @Override
    public String getRemoteApplicationName() {
        return RpcContext.getContext().getRemoteApplicationName();
    }
}
