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
package org.apache.dubbo.rpc.protocol.dubbo.support;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * <code>TestService</code>
 */

public interface DemoService {
    String sayHello(String name);

    Set<String> keys(Map<String, String> map);

    String echo(String text);

    Map echo(Map map);

    CompletableFuture<String> sayHelloAsync(String text);

    long timestamp();

    String getThreadName();

    int getSize(String[] strs);

    int getSize(Object[] os);

    Object invoke(String service, String method) throws Exception;

    int stringLength(String str);

    Type enumlength(Type... types);

    Type getType(Type type);

    String get(CustomArgument arg1);

    int getInt(int arg);

    void nonSerializedParameter(NonSerialized ns);

    NonSerialized returnNonSerialized();

    long add(int a, long b);

    int getPerson(Person person);

    int getPerson(Person person1, Person perso2);

    String getPerson(Man man);

    String getRemoteApplicationName();

    byte[] download(int size);
}