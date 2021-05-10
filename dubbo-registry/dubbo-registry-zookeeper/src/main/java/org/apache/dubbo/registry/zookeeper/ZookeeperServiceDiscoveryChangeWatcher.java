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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged;
import static org.apache.zookeeper.Watcher.Event.EventType.NodeDataChanged;

/**
 * Zookeeper {@link ServiceDiscovery} Change {@link CuratorWatcher watcher} only interests in
 * {@link Watcher.Event.EventType#NodeChildrenChanged} and {@link Watcher.Event.EventType#NodeDataChanged} event types,
 * which will multicast a {@link ServiceInstancesChangedEvent} when the service node has been changed.
 *
 * @since 2.7.5
 */
public class ZookeeperServiceDiscoveryChangeWatcher implements CuratorWatcher {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private ServiceInstancesChangedListener listener;

    private final ZookeeperServiceDiscovery zookeeperServiceDiscovery;

    private final String path;

    private boolean keepWatching = true;

    private final String serviceName;

    public ZookeeperServiceDiscoveryChangeWatcher(ZookeeperServiceDiscovery zookeeperServiceDiscovery,
                                                  String serviceName,
                                                  String path,
                                                  ServiceInstancesChangedListener listener) {
        this.zookeeperServiceDiscovery = zookeeperServiceDiscovery;
        this.serviceName = serviceName;
        this.path = path;
        this.listener = listener;
    }

    @Override
    public void process(WatchedEvent event) throws Exception {

        Watcher.Event.EventType eventType = event.getType();

        if (NodeChildrenChanged.equals(eventType) || NodeDataChanged.equals(eventType)) {
            if (shouldKeepWatching()) {
                List<ServiceInstance> instances = zookeeperServiceDiscovery.getInstances(serviceName);
                listener.onEvent(new ServiceInstancesChangedEvent(serviceName, instances));
                zookeeperServiceDiscovery.dispatchServiceInstancesChangedEvent(serviceName, instances);
                registerSelfWatcher();
            }
        }
    }

    private void registerSelfWatcher() {
        try {
            zookeeperServiceDiscovery.getCuratorFramework().getChildren().usingWatcher(this).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            // ignored
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage());
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public boolean shouldKeepWatching() {
        return keepWatching;
    }

    public void stopWatching() {
        this.keepWatching = false;
    }
}
