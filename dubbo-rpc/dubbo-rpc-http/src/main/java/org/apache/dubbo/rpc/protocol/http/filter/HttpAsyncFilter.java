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
package org.apache.dubbo.rpc.protocol.http.filter;

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * http protocol genericService.$invokeAsync support.
 *
 * @date: 2020-07-26 18:58
 */
@Activate(group = {CommonConstants.CONSUMER})
public class HttpAsyncFilter implements Filter {
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if (!("http".equals(invoker.getUrl().getProtocol())
                && inv.getMethodName().equals(CommonConstants.$INVOKE_ASYNC)
                && inv.getArguments() != null
                && inv.getArguments().length == 3
                && GenericService.class.isAssignableFrom(invoker.getInterface()))) {
            return invoker.invoke(inv);
        }
        try {
            Map<Object, Object> attributes = inv.getAttributes();
            if (attributes != null && attributes.containsKey(Constants.CONSUMER_MODEL)) {
                ConsumerModel consumerModel = (ConsumerModel) attributes.get(Constants.CONSUMER_MODEL);
                inv.getAttributes().put(Constants.METHOD_MODEL, consumerModel.getMethodModel(CommonConstants.$INVOKE));
            }
            RpcInvocation rpcInvocation = new RpcInvocation(CommonConstants.$INVOKE, invoker.getInterface().getName(), inv.getParameterTypes(),
                    inv.getArguments(), inv.getObjectAttachments(),
                    inv.getInvoker(), inv.getAttributes());
            rpcInvocation.setTargetServiceUniqueName(inv.getTargetServiceUniqueName());
            Result result = invoker.invoke(rpcInvocation);
            CompletableFuture<AppResponse> future = CompletableFuture.completedFuture(new AppResponse(result.getValue()));
            return new HttpAsyncRpcResult(future, rpcInvocation);
        } catch (Throwable t) {
            throw new RpcException(t.getMessage(), t);
        }
    }

    class HttpAsyncRpcResult extends AsyncRpcResult {

        public HttpAsyncRpcResult(CompletableFuture<AppResponse> future, Invocation invocation) {
            super(future, invocation);
        }

        @Override
        public Object recreate() throws Throwable {
            Object value = super.recreate();
            if (value instanceof CompletableFuture) {
                return value;
            }
            return CompletableFuture.completedFuture(value);
        }
    }

}