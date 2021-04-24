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
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.rpc.RpcException;

import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static redis.embedded.RedisServer.newRedisServer;

public class RedisMetadataReportTest {

    private static final String
            REDIS_URL_TEMPLATE = "redis://%slocalhost:%d",
            REDIS_PASSWORD = "チェリー",
            REDIS_URL_AUTH_SECTION = "username:" + REDIS_PASSWORD + "@";

    RedisMetadataReport redisMetadataReport;
    RedisMetadataReport syncRedisMetadataReport;
    RedisServer redisServer;
    URL registryUrl;

    @BeforeEach
    public void constructor(final TestInfo testInfo) throws IOException {
        final int redisPort = NetUtils.getAvailablePort();
        final boolean usesAuthentication = usesAuthentication(testInfo);

        redisServer = newRedisServer()
                .port(redisPort)
                .settingIf(usesAuthentication, "requirepass " + REDIS_PASSWORD)
                .settingIf(IS_OS_WINDOWS, "maxheap 128mb")
                .build();
        IOException exception = null;
        for (int i = 0; i < 10; i++) {
            try {
                this.redisServer.start();
            } catch (IOException e) {
                exception = e;
            }
            if (exception == null) {
                break;
            }
        }
        Assertions.assertNull(exception);
        registryUrl = newRedisUrl(usesAuthentication, redisPort);
        redisMetadataReport = (RedisMetadataReport) new RedisMetadataReportFactory().createMetadataReport(registryUrl);
        syncRedisMetadataReport = (RedisMetadataReport) new RedisMetadataReportFactory().createMetadataReport(registryUrl);
    }

    private static boolean usesAuthentication(final TestInfo testInfo) {
        final String methodName = testInfo.getTestMethod().get().getName();
        return "testAuthRedisMetadata".equals(methodName) || "testWrongAuthRedisMetadata".equals(methodName);
    }

    private static URL newRedisUrl(final boolean usesAuthentication, final int redisPort) {
        final String urlAuthSection = usesAuthentication ? REDIS_URL_AUTH_SECTION : "";
        return URL.valueOf(String.format(REDIS_URL_TEMPLATE, urlAuthSection, redisPort));
    }

    @AfterEach
    public void tearDown() throws Exception {
        this.redisServer.stop();
    }

    @Test
    public void testAsyncStoreProvider() throws ClassNotFoundException {
        testStoreProvider(redisMetadataReport, "1.0.0.redis.md.p1", 3000);
    }

    @Test
    public void testSyncStoreProvider() throws ClassNotFoundException {
        testStoreProvider(syncRedisMetadataReport, "1.0.0.redis.md.p2", 3);
    }

    private void testStoreProvider(RedisMetadataReport redisMetadataReport, String version, long moreTime) throws ClassNotFoundException {
        String interfaceName = "org.apache.dubbo.metadata.store.redis.RedisMetadata4TstService";
        String group = null;
        String application = "vic.redis.md";
        MetadataIdentifier providerMetadataIdentifier = storePrivider(redisMetadataReport, interfaceName, version, group, application);
        Jedis jedis = null;
        try {
            jedis = redisMetadataReport.pool.getResource();
            String keyTmp = providerMetadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY);
            String value = jedis.get(keyTmp);
            if (value == null) {
                Thread.sleep(moreTime);
                value = jedis.get(keyTmp);
            }

            Assertions.assertNotNull(value);

            Gson gson = new Gson();
            FullServiceDefinition fullServiceDefinition = gson.fromJson(value, FullServiceDefinition.class);
            Assertions.assertEquals(fullServiceDefinition.getParameters().get("paramTest"), "redisTest");
        } catch (Throwable e) {
            throw new RpcException("Failed to put to redis . cause: " + e.getMessage(), e);
        } finally {
            if (jedis != null) {
                jedis.del(providerMetadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            }
            redisMetadataReport.pool.close();
        }
    }

    private MetadataIdentifier storePrivider(RedisMetadataReport redisMetadataReport, String interfaceName, String version, String group, String application) throws ClassNotFoundException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName + "?paramTest=redisTest&version=" + version + "&application="
                + application + (group == null ? "" : "&group=" + group));

        MetadataIdentifier providerMetadataIdentifier = new MetadataIdentifier(interfaceName, version, group, PROVIDER_SIDE, application);
        Class interfaceClass = Class.forName(interfaceName);
        FullServiceDefinition fullServiceDefinition = ServiceDefinitionBuilder.buildFullDefinition(interfaceClass, url.getParameters());

        redisMetadataReport.storeProviderMetadata(providerMetadataIdentifier, fullServiceDefinition);
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return providerMetadataIdentifier;
    }

    @Test
    public void testAuthRedisMetadata() throws ClassNotFoundException {
        testStoreProvider(redisMetadataReport, "1.0.0.redis.md.p1", 3000);
    }

    @Test
    public void testWrongAuthRedisMetadata() throws ClassNotFoundException {
        registryUrl = registryUrl.setPassword("123456");
        redisMetadataReport = (RedisMetadataReport) new RedisMetadataReportFactory().createMetadataReport(registryUrl);
        try {
            testStoreProvider(redisMetadataReport, "1.0.0.redis.md.p1", 3000);
        } catch (RpcException e) {
            if (e.getCause() instanceof JedisConnectionException && e.getCause().getCause() instanceof JedisDataException) {
                Assertions.assertEquals("ERR invalid password", e.getCause().getCause().getMessage());
            } else {
                Assertions.fail("no invalid password exception!");
            }
        }
    }
}
