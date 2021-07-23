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

/**
 *
 */
public interface Constants {

    String SHARE_CONNECTIONS_KEY = "shareconnections";

    /**
     * By default, a consumer JVM instance and a provider JVM instance share a long TCP connection (except when connections are set),
     * which can set the number of long TCP connections shared to avoid the bottleneck of sharing a single long TCP connection.
     */
    String DEFAULT_SHARE_CONNECTIONS = "1";

    String DECODE_IN_IO_THREAD_KEY = "decode.in.io";

    boolean DEFAULT_DECODE_IN_IO_THREAD = false;

    /**
     * callback inst id
     */
    String CALLBACK_SERVICE_KEY = "callback.service.instid";

    String CALLBACK_SERVICE_PROXY_KEY = "callback.service.proxy";

    String IS_CALLBACK_SERVICE = "is_callback_service";

    /**
     * Invokers in channel's callback
     */
    String CHANNEL_CALLBACK_KEY = "channel.callback.invokers.key";

    /**
     * The initial connect state for lazy connection
     */
    String LAZY_CONNECT_INITIAL_STATE_KEY = "connect.lazy.initial.state";

    /**
     * The default value of lazy connection's initial connect state: true
     *
     * @see #LAZY_CONNECT_INITIAL_STATE_KEY
     */
    boolean DEFAULT_LAZY_CONNECT_INITIAL_STATE = true;

    /**
     * The close state for lazy connection
     */
    String LAZY_CLOSE_STATE_KEY = "close.lazy.state";

    String OPTIMIZER_KEY = "optimizer";

    String ON_CONNECT_KEY = "onconnect";

    String ON_DISCONNECT_KEY = "ondisconnect";

    String ASYNC_METHOD_INFO = "async-method-info";

}
