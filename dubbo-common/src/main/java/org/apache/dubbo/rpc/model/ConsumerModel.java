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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.BaseServiceMetadata;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.ReferenceConfigBase;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * This model is bound to your reference's configuration, for example, group, version or method level configuration.
 */
public class ConsumerModel {
    private String serviceKey;
    private final ServiceDescriptor serviceModel;
    private final ReferenceConfigBase<?> referenceConfig;
    private final Set<String> apps = new TreeSet<>();

    private Object proxyObject;

    private Map<String, AsyncMethodInfo> methodConfigs = new HashMap<>();

    /**
     *  This constructor create an instance of ConsumerModel and passed objects should not be null.
     *  If service name, service instance, proxy object,methods should not be null. If these are null
     *  then this constructor will throw {@link IllegalArgumentException}
     * @param serviceKey Name of the service.
     * @param proxyObject  Proxy object.
     */
    public ConsumerModel(String serviceKey
            , Object proxyObject
            , ServiceDescriptor serviceModel
            , ReferenceConfigBase<?> referenceConfig) {

        Assert.notEmptyString(serviceKey, "Service name can't be null or blank");

        this.serviceKey = serviceKey;
        this.proxyObject = proxyObject;
        this.serviceModel = serviceModel;
        this.referenceConfig = referenceConfig;
    }

    public void init(Map<String, AsyncMethodInfo> attributes) {
        if (attributes != null) {
            this.methodConfigs = attributes;
        }

        initMethodModels();
    }

    /**
     * Return the proxy object used by called while creating instance of ConsumerModel
     * @return
     */
    public Object getProxyObject() {
        return proxyObject;
    }

    public void setProxyObject(Object proxyObject) {
        this.proxyObject = proxyObject;
    }

    /**
     * Return all method models for the current service
     *
     * @return method model list
     */
    public Set<MethodDescriptor> getAllMethods() {
        return serviceModel.getAllMethods();
    }

    public Class<?> getServiceInterfaceClass() {
        return serviceModel.getServiceInterfaceClass();
    }

    public String getServiceKey() {
        return serviceKey;
    }

    public AsyncMethodInfo getMethodConfig(String methodName) {
        return methodConfigs.get(methodName);
    }

    public ServiceDescriptor getServiceModel() {
        return serviceModel;
    }

    public ReferenceConfigBase getReferenceConfig() {
        return referenceConfig;
    }

    public Set<String> getApps() {
        return apps;
    }

    public AsyncMethodInfo getAsyncInfo(String methodName) {
        return methodConfigs.get(methodName);
    }

    /* *************** Start, metadata compatible **************** */

    private ServiceMetadata serviceMetadata;
    private Map<Method, MethodDescriptor> methodModels = new HashMap<>();

    public ConsumerModel(String serviceKey
            , Object proxyObject
            , ServiceDescriptor serviceModel
            , ReferenceConfigBase<?> referenceConfig
            , ServiceMetadata metadata) {

        this(serviceKey, proxyObject, serviceModel, referenceConfig);
        this.serviceMetadata = metadata;
    }

    public void setServiceKey(String serviceKey) {
        this.serviceKey = serviceKey;
        if (serviceMetadata != null) {
            serviceMetadata.setServiceKey(serviceKey);
            serviceMetadata.setGroup(BaseServiceMetadata.groupFromServiceKey(serviceKey));
        }
    }

    public void initMethodModels() {
        Class[] interfaceList = null;
        if (proxyObject == null) {
            interfaceList = new Class[]{referenceConfig.getActualInterface()};
        } else {
            interfaceList = proxyObject.getClass().getInterfaces();
        }
        for (Class interfaceClass : interfaceList) {
            for (Method method : interfaceClass.getMethods()) {
                methodModels.put(method, new MethodDescriptor(method));
            }
        }
    }

    public ClassLoader getClassLoader() {
        return serviceMetadata.getServiceType().getClassLoader();
    }

    /**
     * @return serviceMetadata
     */
    public ServiceMetadata getServiceMetadata() {
        return serviceMetadata;
    }

    /**
     * Return method model for the given method on consumer side
     *
     * @param method method object
     * @return method model
     */
    public MethodDescriptor getMethodModel(Method method) {
        return methodModels.get(method);
    }

    /**
     * Return method model for the given method on consumer side
     *
     * @param method method object
     * @return method model
     */
    public MethodDescriptor getMethodModel(String method) {
        Optional<Map.Entry<Method, MethodDescriptor>> consumerMethodModelEntry = methodModels.entrySet().stream().filter(entry -> entry.getKey().getName().equals(method)).findFirst();
        return consumerMethodModelEntry.map(Map.Entry::getValue).orElse(null);
    }

    /**
     * @param method   methodName
     * @param argsType method arguments type
     * @return
     */
    public MethodDescriptor getMethodModel(String method, String[] argsType) {
        Optional<MethodDescriptor> consumerMethodModel = methodModels.entrySet().stream()
                .filter(entry -> entry.getKey().getName().equals(method))
                .map(Map.Entry::getValue).filter(methodModel -> Arrays.equals(argsType, methodModel.getCompatibleParamSignatures()))
                .findFirst();
        return consumerMethodModel.orElse(null);
    }

    /**
     * Return all method models for the current service
     *
     * @return method model list
     */
    public List<MethodDescriptor> getAllMethodModels() {
        return new ArrayList<>(methodModels.values());
    }

    public String getServiceName() {
        return this.serviceMetadata.getServiceKey();
    }


}
