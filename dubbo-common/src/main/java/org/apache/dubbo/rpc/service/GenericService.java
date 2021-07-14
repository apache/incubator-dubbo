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
package org.apache.dubbo.rpc.service;

import java.util.concurrent.CompletableFuture;

/**
 * Generic service interface
 *
 * @export
 */
public interface GenericService {

    /**
     * Generic invocation
     *
     * @param method         Method name, e.g. findPerson. If there are overridden methods, parameter info is
     *                       required, e.g. findPerson(java.lang.String)
     * @param parameterTypes Parameter types
     * @param args           Arguments
     * @return invocation return value
     * @throws GenericException potential exception thrown from the invocation
     */
    Object $invoke(String method, String[] parameterTypes, Object[] args) throws GenericException;

    /**
     * Call target method on the consumer side asynchronously, the provider side should not be called the $invokeAsync method.
     *
     * @param method         Method name, e.g. findPerson. If there are overridden methods, parameter info is
     *                       required, e.g. findPerson(java.lang.String)
     * @param parameterTypes Parameter types
     * @param args           Arguments
     * @return invocation return value
     * @throws GenericException potential exception thrown from the invocation
     */
    default CompletableFuture<Object> $invokeAsync(String method, String[] parameterTypes, Object[] args) throws GenericException {
        // Use a custom Executor?
        return CompletableFuture.supplyAsync(() -> {
            Object object = $invoke(method, parameterTypes, args);
            try {
                if (object instanceof CompletableFuture) {
                    return ((CompletableFuture) object).get();
                }
            } catch (Exception e) {
                throw new GenericException(e);
            }
            return object;
        });
    }

}
