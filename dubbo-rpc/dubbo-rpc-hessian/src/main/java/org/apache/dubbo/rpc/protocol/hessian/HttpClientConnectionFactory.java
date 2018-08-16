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
package org.apache.dubbo.rpc.protocol.hessian;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.rpc.RpcContext;

import com.caucho.hessian.client.HessianConnection;
import com.caucho.hessian.client.HessianConnectionFactory;
import com.caucho.hessian.client.HessianProxyFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.HttpConnectionParams;

import java.io.IOException;
import java.net.URL;

/**
 * HttpClientConnectionFactory
 */
public class HttpClientConnectionFactory implements HessianConnectionFactory {

    private final HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

    private HessianProxyFactory hessianProxyFactory;

    @Override
    public void setHessianProxyFactory(HessianProxyFactory factory) {
        this.hessianProxyFactory = factory;
    }

    @Override
    public HessianConnection open(URL url) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout((int) this.hessianProxyFactory.getConnectTimeout())
                .setSocketTimeout((int) this.hessianProxyFactory.getReadTimeout())
                .build();
        HttpClient httpClient = httpClientBuilder.setDefaultRequestConfig(requestConfig).build();
        HttpClientConnection httpClientConnection = new HttpClientConnection(httpClient, url);
        RpcContext context = RpcContext.getContext();
        for (String key : context.getAttachments().keySet()) {
            httpClientConnection.addHeader(Constants.DEFAULT_EXCHANGER + key, context.getAttachment(key));
        }
        return httpClientConnection;
    }
}
