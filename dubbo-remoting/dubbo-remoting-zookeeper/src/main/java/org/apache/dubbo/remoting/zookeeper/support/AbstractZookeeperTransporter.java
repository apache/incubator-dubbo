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
package org.apache.dubbo.remoting.zookeeper.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.RemotingConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * AbstractZookeeperTransporter is abstract implements of ZookeeperTransporter.
 * <p>
 * If you want to extends this, implements createZookeeperClient.
 */
public abstract class AbstractZookeeperTransporter implements ZookeeperTransporter {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperTransporter.class);
    private final Map<String, ZookeeperClient> zookeeperClientMap = new ConcurrentHashMap<>();

    /**
     * share connect for registry, metadata, etc..
     * <p>
     * Make sure the connection is connected.
     *
     * @param url
     * @return
     */
    @Override
    public ZookeeperClient connect(URL url) {
        ZookeeperClient zookeeperClient;
        // address format: {[username:password@]address}
        String addressListStr = getURLBackupAddressStr(url);

        // The field define the zookeeper server , including protocol, host, port, username, password
        if ((zookeeperClient = getCachedConnectedClient(addressListStr)) != null) {
            logger.info("find valid zookeeper client from the cache for address: " + url);
            return zookeeperClient;
        }
        // avoid creating too many connections, so add lock
        synchronized (zookeeperClientMap) {
            if ((zookeeperClient = getCachedConnectedClient(addressListStr)) != null) {
                logger.info("find valid zookeeper client from the cache for address: " + url);
                return zookeeperClient;
            }

            zookeeperClient = createZookeeperClient(url);
            logger.info("No valid zookeeper client found from cache, therefore create a new client for url. " + url);
            writeToClientMap(addressListStr, zookeeperClient);
        }
        return zookeeperClient;
    }

    /**
     * @param url the url that will create zookeeper connection .
     *            The url in AbstractZookeeperTransporter#connect parameter is rewritten by this one.
     *            such as: zookeeper://127.0.0.1:2181/org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter
     * @return
     */
    protected abstract ZookeeperClient createZookeeperClient(URL url);

    /**
     * get the ZookeeperClient from cache, the ZookeeperClient must be connected.
     * <p>
     * It is not private method for unit test.
     *
     * @param addressListStr
     * @return
     */
    ZookeeperClient getCachedConnectedClient(String addressListStr) {

        ZookeeperClient zookeeperClient = zookeeperClientMap.get(addressListStr);

        if (zookeeperClient != null && zookeeperClient.isConnected()) {
            return zookeeperClient;
        }
        return null;
    }

    String getURLBackupAddressStr(URL url) {
        List<String> addressList = getURLBackupAddress(url);
        Collections.sort(addressList);
        return StringUtils.join(addressList, ",");
    }

    /**
     * get all zookeeper urls (such as :zookeeper://127.0.0.1:2181?127.0.0.1:8989,127.0.0.1:9999)
     *
     * @param url such as:zookeeper://127.0.0.1:2181?127.0.0.1:8989,127.0.0.1:9999
     * @return such as 127.0.0.1:2181,127.0.0.1:8989,127.0.0.1:9999
     */
    List<String> getURLBackupAddress(URL url) {
        List<String> addressList = new ArrayList<String>();
        addressList.add(url.getAddress());
        addressList.addAll(url.getParameter(RemotingConstants.BACKUP_KEY, Collections.EMPTY_LIST));

        String authPrefix = null;
        if (StringUtils.isNotEmpty(url.getUsername())) {
            StringBuilder buf = new StringBuilder();
            buf.append(url.getUsername());
            if (StringUtils.isNotEmpty(url.getPassword())) {
                buf.append(":");
                buf.append(url.getPassword());
            }
            buf.append("@");
            authPrefix = buf.toString();
        }

        if (StringUtils.isNotEmpty(authPrefix)) {
            List<String> authedAddressList = new ArrayList<>(addressList.size());
            for (String addr : addressList) {
                authedAddressList.add(authPrefix + addr);
            }
            return authedAddressList;
        }


        return addressList;
    }

    /**
     * write address-ZookeeperClient relationship to Map
     *
     * @param addressListStr
     * @param zookeeperClient
     */
    void writeToClientMap(String addressListStr, ZookeeperClient zookeeperClient) {
        zookeeperClientMap.put(addressListStr, zookeeperClient);
    }

    /**
     * redefine the url for zookeeper. just keep protocol, username, password, host, port, and individual parameter.
     *
     * @param url
     * @return
     */
    URL toClientURL(URL url) {
        Map<String, String> parameterMap = new HashMap<>();
        // for CuratorZookeeperClient
        if (url.getParameter(TIMEOUT_KEY) != null) {
            parameterMap.put(TIMEOUT_KEY, url.getParameter(TIMEOUT_KEY));
        }
        if (url.getParameter(RemotingConstants.BACKUP_KEY) != null) {
            parameterMap.put(RemotingConstants.BACKUP_KEY, url.getParameter(RemotingConstants.BACKUP_KEY));
        }

        return new URL(url.getProtocol(), url.getUsername(), url.getPassword(), url.getHost(), url.getPort(),
                ZookeeperTransporter.class.getName(), parameterMap);
    }

    /**
     * for unit test
     *
     * @return
     */
    Map<String, ZookeeperClient> getZookeeperClientMap() {
        return zookeeperClientMap;
    }
}
