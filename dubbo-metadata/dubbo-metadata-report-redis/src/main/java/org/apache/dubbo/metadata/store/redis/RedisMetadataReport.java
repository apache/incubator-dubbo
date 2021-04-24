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
package org.apache.dubbo.metadata.store.redis;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.report.identifier.BaseMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.CLUSTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.metadata.MetadataConstants.META_DATA_STORE_TAG;

/**
 * RedisMetadataReport
 */
public class RedisMetadataReport extends AbstractMetadataReport {

    private final static String REDIS_DATABASE_KEY = "database";
    /**
     * maximum number of retries
     */
    private final static int MAX_ATTEMPTS = 2;
    /**
     * the default slot of the redis database
     */
    private final static int DEFAULT_REDIS_DATABASE_SLOT = 0;

    JedisPool pool;
    Set<HostAndPort> jedisClusterNodes;
    private int timeout;
    private String password;


    public RedisMetadataReport(URL url) {
        super(url);
        timeout = url.getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
        password = url.getPassword();
        if (url.getParameter(CLUSTER_KEY, false)) {
            jedisClusterNodes = new HashSet<HostAndPort>();
            List<URL> urls = url.getBackupUrls();
            for (URL tmpUrl : urls) {
                jedisClusterNodes.add(new HostAndPort(tmpUrl.getHost(), tmpUrl.getPort()));
            }
        } else {
            int database = url.getParameter(REDIS_DATABASE_KEY, DEFAULT_REDIS_DATABASE_SLOT);
            pool = new JedisPool(new JedisPoolConfig(), url.getHost(), url.getPort(), timeout, password, database);
        }
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        this.storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier, URL url) {
        this.storeMetadata(serviceMetadataIdentifier, URL.encode(url.toFullString()));
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier) {
        this.deleteMetadata(serviceMetadataIdentifier);
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        String content = getMetadata(metadataIdentifier);
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(Arrays.asList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        this.storeMetadata(subscriberMetadataIdentifier, urlListStr);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        return this.getMetadata(subscriberMetadataIdentifier);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return this.getMetadata(metadataIdentifier);
    }

    private void storeMetadata(BaseMetadataIdentifier metadataIdentifier, String v) {
        if (pool != null) {
            storeMetadataStandalone(metadataIdentifier, v);
        } else {
            storeMetadataInCluster(metadataIdentifier, v);
        }
    }

    private void storeMetadataInCluster(BaseMetadataIdentifier metadataIdentifier, String v) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, MAX_ATTEMPTS, password,
                new GenericObjectPoolConfig())) {
            jedisCluster.set(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG, v);
        } catch (Throwable e) {
            logger.error("Failed to put " + metadataIdentifier + " to redis cluster " + v + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + metadataIdentifier + " to redis cluster " + v + ", cause: " + e.getMessage(), e);
        }
    }

    private void storeMetadataStandalone(BaseMetadataIdentifier metadataIdentifier, String v) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), v);
        } catch (Throwable e) {
            logger.error("Failed to put " + metadataIdentifier + " to redis " + v + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + metadataIdentifier + " to redis " + v + ", cause: " + e.getMessage(), e);
        }
    }

    private void deleteMetadata(BaseMetadataIdentifier metadataIdentifier) {
        if (pool != null) {
            deleteMetadataStandalone(metadataIdentifier);
        } else {
            deleteMetadataInCluster(metadataIdentifier);
        }
    }

    private void deleteMetadataInCluster(BaseMetadataIdentifier metadataIdentifier) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, MAX_ATTEMPTS, password,
                new GenericObjectPoolConfig())) {
            jedisCluster.del(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG);
        } catch (Throwable e) {
            logger.error("Failed to delete " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to delete " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
        }
    }

    private void deleteMetadataStandalone(BaseMetadataIdentifier metadataIdentifier) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable e) {
            logger.error("Failed to delete " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to delete " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
        }
    }

    private String getMetadata(BaseMetadataIdentifier metadataIdentifier) {
        if (pool != null) {
            return getMetadataStandalone(metadataIdentifier);
        } else {
            return getMetadataInCluster(metadataIdentifier);
        }
    }

    private String getMetadataInCluster(BaseMetadataIdentifier metadataIdentifier) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, MAX_ATTEMPTS, password,
                new GenericObjectPoolConfig())) {
            return jedisCluster.get(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG);
        } catch (Throwable e) {
            logger.error("Failed to get " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to get " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
        }
    }

    private String getMetadataStandalone(BaseMetadataIdentifier metadataIdentifier) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.get(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable e) {
            logger.error("Failed to get " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to get " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
        }
    }

}
