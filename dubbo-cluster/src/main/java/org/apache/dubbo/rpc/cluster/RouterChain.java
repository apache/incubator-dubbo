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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.state.AddrCache;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.RouterCache;
import org.apache.dubbo.rpc.cluster.router.state.StateRouter;
import org.apache.dubbo.rpc.cluster.router.state.StateRouterFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.STATE_ROUTER_KEY;

/**
 * Router chain
 */
public class RouterChain<T> {
    private static final Logger logger = LoggerFactory.getLogger(RouterChain.class);

    // full list of addresses from registry, classified by method name.
    private volatile List<Invoker<T>> invokers = Collections.emptyList();

    // containing all routers, reconstruct every time 'route://' urls change.
    private volatile List<Router> routers = Collections.emptyList();

    // Fixed router instances: ConfigConditionRouter, TagRouter, e.g., the rule for each instance may change but the
    // instance will never delete or recreate.
    private List<Router> builtinRouters = Collections.emptyList();

    private List<StateRouter> builtinStateRouters = Collections.emptyList();
    private List<StateRouter> stateRouters = Collections.emptyList();
    private final ExecutorRepository executorRepository = ExtensionLoader.getExtensionLoader(ExecutorRepository.class)
        .getDefaultExtension();

    protected URL url;

    private AtomicReference<AddrCache<T>> cache = new AtomicReference<>();

    private final Semaphore loopPermit = new Semaphore(1);
    private final Semaphore loopPermitNotify = new Semaphore(1);

    private final ExecutorService loopPool;

    private AtomicBoolean firstBuildCache = new AtomicBoolean(true);

    public static <T> RouterChain<T> buildChain(URL url) {
        return new RouterChain<>(url);
    }

    private RouterChain(URL url) {
        loopPool = executorRepository.nextExecutorExecutor();
        List<RouterFactory> extensionFactories = ExtensionLoader.getExtensionLoader(RouterFactory.class)
            .getActivateExtension(url, ROUTER_KEY);

        List<Router> routers = extensionFactories.stream()
            .map(factory -> factory.getRouter(url))
            .collect(Collectors.toList());

        initWithRouters(routers);

        List<StateRouterFactory> extensionStateRouterFactories = ExtensionLoader.getExtensionLoader(
            StateRouterFactory.class)
            .getActivateExtension(url, STATE_ROUTER_KEY);

        List<StateRouter> stateRouters = extensionStateRouterFactories.stream()
            .map(factory -> factory.getRouter(url, this))
            .sorted(StateRouter::compareTo)
            .collect(Collectors.toList());

        // init state routers
        initWithStateRouters(stateRouters);
    }

    /**
     * the resident routers must being initialized before address notification.
     * FIXME: this method should not be public
     */
    public void initWithRouters(List<Router> builtinRouters) {
        this.builtinRouters = builtinRouters;
        this.routers = new ArrayList<>(builtinRouters);
        this.sort();
    }

    private void initWithStateRouters(List<StateRouter> builtinRouters) {
        this.builtinStateRouters = builtinRouters;
        this.stateRouters = new ArrayList<>(builtinRouters);
    }

    /**
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     */
    public void addRouters(List<Router> routers) {
        List<Router> newRouters = new ArrayList<>();
        newRouters.addAll(builtinRouters);
        newRouters.addAll(routers);
        CollectionUtils.sort(newRouters);
        this.routers = newRouters;
    }

    public void addStateRouters(List<StateRouter> stateRouters) {
        List<StateRouter> newStateRouters = new ArrayList<>();
        newStateRouters.addAll(builtinStateRouters);
        newStateRouters.addAll(stateRouters);
        CollectionUtils.sort(newStateRouters);
        this.stateRouters = newStateRouters;
    }

    public List<Router> getRouters() {
        return routers;
    }

    public List<StateRouter> getStateRouters() {
        return stateRouters;
    }

    private void sort() {
        Collections.sort(routers);
    }

    /**
     * @param url
     * @param invocation
     * @return
     */
    public List<Invoker<T>> route(URL url, Invocation invocation) {

        AddrCache<T> cache = this.cache.get();
        if (cache == null) {
            throw new RpcException(RpcException.ROUTER_CACHE_NOT_BUILD, "Failed to invoke the method "
                + invocation.getMethodName() + " in the service " + url.getServiceInterface()
                + ". address cache not build "
                + " on the consumer " + NetUtils.getLocalHost()
                + " using the dubbo version " + Version.getVersion()
                + ".");
        }
        BitList<Invoker<T>> finalBitListInvokers = new BitList<>(invokers, false);
        for (StateRouter stateRouter : stateRouters) {
            if (stateRouter.isEnable()) {
                RouterCache<T> routerCache = cache.getCache().get(stateRouter.getName());
                finalBitListInvokers = stateRouter.route(finalBitListInvokers, routerCache, url, invocation);
            }
        }

        List<Invoker<T>> finalInvokers = new ArrayList<>(finalBitListInvokers.size());

        for(Invoker<T> invoker: finalBitListInvokers) {
            finalInvokers.add(invoker);
        }

        for (Router router : routers) {
            finalInvokers = router.route(finalInvokers, url, invocation);
        }
        return finalInvokers;
    }

    /**
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = (invokers == null ? Collections.emptyList() : invokers);
        stateRouters.forEach(router -> router.notify(this.invokers));
        routers.forEach(router -> router.notify(this.invokers));
        loop(true);
    }

    /**
     * Build the asynchronous address cache for stateRouter.
     * @param notify Whether the addresses in registry has changed.
     */
    private void buildCache(boolean notify) {
        if (invokers == null || invokers.size() <= 0) {
            return;
        }
        AddrCache<T> origin = cache.get();
        List<Invoker<T>> copyInvokers = new ArrayList<>(this.invokers);
        AddrCache<T> newCache = new AddrCache<T>();
        Map<String, RouterCache<T>> routerCacheMap = new HashMap<>((int) (stateRouters.size() / 0.75f) + 1);
        newCache.setInvokers(invokers);
        for (StateRouter stateRouter : stateRouters) {
            try {
                RouterCache routerCache = poolRouter(stateRouter, origin, copyInvokers, notify);
                //file cache
                routerCacheMap.put(stateRouter.getName(), routerCache);
            } catch (Throwable t) {
                logger.error("Failed to pool router: " + stateRouter.getUrl() + ", cause: " + t.getMessage(), t);
                return;
            }
        }

        newCache.setCache(routerCacheMap);
        this.cache.set(newCache);
    }

    /**
     * Cache the address list for each StateRouter.
     * @param router router
     * @param orign The original address cache
     * @param invokers The full address list
     * @param notify Whether the addresses in registry has changed.
     * @return
     */
    private RouterCache poolRouter(StateRouter router, AddrCache<T> orign, List<Invoker<T>> invokers, boolean notify) {
        String routerName = router.getName();
        RouterCache routerCache;
        if (isCacheMiss(orign, routerName) || router.shouldRePool() || notify) {
            return router.pool(invokers);
        } else {
            routerCache = orign.getCache().get(routerName);
        }
        if (routerCache == null) {
            return new RouterCache();
        }
        return routerCache;
    }

    private boolean isCacheMiss(AddrCache<T> cache, String routerName) {
        return cache == null || cache.getCache() == null || cache.getInvokers() == null || cache.getCache().get(
            routerName)
            == null;
    }

    /***
     * Build the asynchronous address cache for stateRouter.
     * @param notify Whether the addresses in registry has changed.
     */
    public void loop(boolean notify) {
        if (firstBuildCache.get()) {
            firstBuildCache.compareAndSet(true,false);
            buildCache(notify);
        }
        if (notify) {
            if (loopPermitNotify.tryAcquire()) {
                loopPool.submit(new NotifyLoopRunnable(true, loopPermitNotify));
            }
        } else {
            if (loopPermit.tryAcquire()) {
                loopPool.submit(new NotifyLoopRunnable(false, loopPermit));
            }
        }
    }

    class NotifyLoopRunnable implements Runnable {

        private final boolean notify;
        private final Semaphore loopPermit;

        public NotifyLoopRunnable(boolean notify, Semaphore loopPermit) {
            this.notify = notify;
            this.loopPermit = loopPermit;
        }

        @Override
        public void run() {
            loopPermit.release();
            buildCache(notify);
        }
    }

    public void destroy() {
        invokers = Collections.emptyList();
        for (Router router : routers) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error("Error trying to stop router " + router.getClass(), e);
            }
        }
        routers = Collections.emptyList();
        builtinRouters = Collections.emptyList();

        for (StateRouter router : stateRouters) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error("Error trying to stop stateRouter " + router.getClass(), e);
            }
        }
        stateRouters = Collections.emptyList();
        builtinStateRouters = Collections.emptyList();
    }

}
