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
package org.apache.dubbo.config.context;

import org.apache.dubbo.common.config.CompositeConfiguration;
import org.apache.dubbo.common.context.FrameworkExt;
import org.apache.dubbo.common.context.LifecycleAdapter;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.AbstractInterfaceConfig;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.ConfigKeys;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.MetricsConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.MonitorConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.SslConfig;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static org.apache.dubbo.common.utils.StringUtils.isNotEmpty;
import static org.apache.dubbo.config.AbstractConfig.getTagName;

/**
 * A lock-free config manager (through ConcurrentHashMap), for fast read operation.
 * The Write operation lock with sub configs map of config type, for safely check and add new config.
 */
public class ConfigManager extends LifecycleAdapter implements FrameworkExt {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    public static final String NAME = "config";
    public static final String BEAN_NAME = "dubboConfigManager";
    private static final String CONFIG_NAME_READ_METHOD = "getName";
    public static final String DUBBO_CONFIG_MODE = ConfigKeys.DUBBO_CONFIG_MODE;

    final Map<String, Map<String, AbstractConfig>> configsCache = new ConcurrentHashMap<>();

    private Map<String, AbstractInterfaceConfig> referenceConfigCache = new ConcurrentHashMap<>();

    private Map<String, AbstractInterfaceConfig> serviceConfigCache = new ConcurrentHashMap<>();

    private Set<AbstractConfig> duplicatedConfigs = new ConcurrentHashSet<>();

    private ConfigMode configMode = ConfigMode.STRICT;

    private boolean ignoreDuplicatedInterface = false;

    private static Map<String, AtomicInteger> configIdIndexes = new ConcurrentHashMap<>();

    private static Set<Class<? extends AbstractConfig>> uniqueConfigTypes = new ConcurrentHashSet<>();

    static {
        // init unique config types
        uniqueConfigTypes.add(ApplicationConfig.class);
        uniqueConfigTypes.add(ModuleConfig.class);
        uniqueConfigTypes.add(MonitorConfig.class);
        uniqueConfigTypes.add(MetricsConfig.class);
        uniqueConfigTypes.add(SslConfig.class);

        List<String> configNames = new ArrayList<>(uniqueConfigTypes.size());
        for (Class<? extends AbstractConfig> configType : uniqueConfigTypes) {
            configNames.add(configType.getSimpleName());
        }
    }

    public ConfigManager() {
    }

    @Override
    public void initialize() throws IllegalStateException {
        CompositeConfiguration configuration = ApplicationModel.getEnvironment().getConfiguration();
        String configModeStr = (String) configuration.getProperty(DUBBO_CONFIG_MODE);
        try {
            if (StringUtils.hasText(configModeStr)) {
                this.configMode = ConfigMode.valueOf(configModeStr.toUpperCase());
            }
        } catch (Exception e) {
            String msg = "Illegal '" + DUBBO_CONFIG_MODE + "' config value [" + configModeStr + "], available values " + Arrays.toString(ConfigMode.values());
            logger.error(msg, e);
            throw new IllegalArgumentException(msg, e);
        }

        String ignoreDuplicatedInterfaceStr = (String) configuration
            .getProperty(ConfigKeys.DUBBO_CONFIG_IGNORE_DUPLICATED_INTERFACE);
        if (ignoreDuplicatedInterfaceStr != null) {
            this.ignoreDuplicatedInterface = Boolean.parseBoolean(ignoreDuplicatedInterfaceStr);
        }
        logger.info("Dubbo config mode: " + configMode +", ignore duplicated interface: " + ignoreDuplicatedInterface);
    }


// ApplicationConfig correlative methods

    /**
     * Set application config
     * @param application
     * @return current application config instance
     */
    @DisableInject
    public void setApplication(ApplicationConfig application) {
        addConfig(application, true);
    }

    public Optional<ApplicationConfig> getApplication() {
        return ofNullable(getSingleConfig(getTagName(ApplicationConfig.class)));
    }

    public ApplicationConfig getApplicationOrElseThrow() {
        return getApplication().orElseThrow(() -> new IllegalStateException("There's no ApplicationConfig specified."));
    }

    // MonitorConfig correlative methods

    @DisableInject
    public void setMonitor(MonitorConfig monitor) {
        addConfig(monitor, true);
    }

    public Optional<MonitorConfig> getMonitor() {
        return ofNullable(getSingleConfig(getTagName(MonitorConfig.class)));
    }

    // ModuleConfig correlative methods

    @DisableInject
    public void setModule(ModuleConfig module) {
        addConfig(module, true);
    }

    public Optional<ModuleConfig> getModule() {
        return ofNullable(getSingleConfig(getTagName(ModuleConfig.class)));
    }

    @DisableInject
    public void setMetrics(MetricsConfig metrics) {
        addConfig(metrics, true);
    }

    public Optional<MetricsConfig> getMetrics() {
        return ofNullable(getSingleConfig(getTagName(MetricsConfig.class)));
    }

    @DisableInject
    public void setSsl(SslConfig sslConfig) {
        addConfig(sslConfig, true);
    }

    public Optional<SslConfig> getSsl() {
        return ofNullable(getSingleConfig(getTagName(SslConfig.class)));
    }

    // ConfigCenterConfig correlative methods

    public void addConfigCenter(ConfigCenterConfig configCenter) {
        addConfig(configCenter);
    }

    public void addConfigCenters(Iterable<ConfigCenterConfig> configCenters) {
        configCenters.forEach(this::addConfigCenter);
    }

    public Optional<Collection<ConfigCenterConfig>> getDefaultConfigCenter() {
        Collection<ConfigCenterConfig> defaults = getDefaultConfigs(getConfigsMap(getTagName(ConfigCenterConfig.class)));
        if (CollectionUtils.isEmpty(defaults)) {
            defaults = getConfigCenters();
        }
        return Optional.ofNullable(defaults);
    }

    public Optional<ConfigCenterConfig> getConfigCenter(String id) {
        return getConfig(ConfigCenterConfig.class, id);
    }

    public Collection<ConfigCenterConfig> getConfigCenters() {
        return getConfigs(getTagName(ConfigCenterConfig.class));
    }

    // MetadataReportConfig correlative methods

    public void addMetadataReport(MetadataReportConfig metadataReportConfig) {
        addConfig(metadataReportConfig);
    }

    public void addMetadataReports(Iterable<MetadataReportConfig> metadataReportConfigs) {
        metadataReportConfigs.forEach(this::addMetadataReport);
    }

    public Collection<MetadataReportConfig> getMetadataConfigs() {
        return getConfigs(getTagName(MetadataReportConfig.class));
    }

    public Collection<MetadataReportConfig> getDefaultMetadataConfigs() {
        Collection<MetadataReportConfig> defaults = getDefaultConfigs(getConfigsMap(getTagName(MetadataReportConfig.class)));
        if (CollectionUtils.isEmpty(defaults)) {
            return getMetadataConfigs();
        }
        return defaults;
    }

    // MetadataReportConfig correlative methods

    public void addProvider(ProviderConfig providerConfig) {
        addConfig(providerConfig);
    }

    public void addProviders(Iterable<ProviderConfig> providerConfigs) {
        providerConfigs.forEach(this::addProvider);
    }

    public Optional<ProviderConfig> getProvider(String id) {
        return getConfig(ProviderConfig.class, id);
    }

    /**
     * Only allows one default ProviderConfig
     */
    public Optional<ProviderConfig> getDefaultProvider() {
        List<ProviderConfig> providerConfigs = getDefaultConfigs(getConfigsMap(getTagName(ProviderConfig.class)));
        if (CollectionUtils.isNotEmpty(providerConfigs)) {
            return Optional.of(providerConfigs.get(0));
        }
        return Optional.empty();
    }

    public Collection<ProviderConfig> getProviders() {
        return getConfigs(getTagName(ProviderConfig.class));
    }

    // ConsumerConfig correlative methods

    public void addConsumer(ConsumerConfig consumerConfig) {
        addConfig(consumerConfig);
    }

    public void addConsumers(Iterable<ConsumerConfig> consumerConfigs) {
        consumerConfigs.forEach(this::addConsumer);
    }

    public Optional<ConsumerConfig> getConsumer(String id) {
        return getConfig(ConsumerConfig.class, id);
    }

    /**
     * Only allows one default ConsumerConfig
     */
    public Optional<ConsumerConfig> getDefaultConsumer() {
        List<ConsumerConfig> consumerConfigs = getDefaultConfigs(getConfigsMap(getTagName(ConsumerConfig.class)));
        if (CollectionUtils.isNotEmpty(consumerConfigs)) {
            return Optional.of(consumerConfigs.get(0));
        }
        return Optional.empty();
    }

    public Collection<ConsumerConfig> getConsumers() {
        return getConfigs(getTagName(ConsumerConfig.class));
    }

    // ProtocolConfig correlative methods

    public void addProtocol(ProtocolConfig protocolConfig) {
        addConfig(protocolConfig);
    }

    public void addProtocols(Iterable<ProtocolConfig> protocolConfigs) {
        if (protocolConfigs != null) {
            protocolConfigs.forEach(this::addProtocol);
        }
    }

    public Optional<ProtocolConfig> getProtocol(String idOrName) {
        return getConfig(ProtocolConfig.class, idOrName);
    }

    public List<ProtocolConfig> getDefaultProtocols() {
        return getDefaultConfigs(ProtocolConfig.class);
    }

    public <C extends AbstractConfig> List<C> getDefaultConfigs(Class<C> cls) {
        return getDefaultConfigs(getConfigsMap(getTagName(cls)));
    }

    public Collection<ProtocolConfig> getProtocols() {
        return getConfigs(getTagName(ProtocolConfig.class));
    }


    // RegistryConfig correlative methods

    public void addRegistry(RegistryConfig registryConfig) {
        addConfig(registryConfig);
    }

    public void addRegistries(Iterable<RegistryConfig> registryConfigs) {
        if (registryConfigs != null) {
            registryConfigs.forEach(this::addRegistry);
        }
    }

    public Optional<RegistryConfig> getRegistry(String id) {
        return getConfig(RegistryConfig.class, id);
    }

    /**
     * Get config instance by id or by name
     * @param cls Config type
     * @param idOrName  the id or name of the config
     * @return
     */
    public <T extends AbstractConfig> Optional<T> getConfig(Class<T> cls, String idOrName) {
        T config = getConfigById(getTagName(cls), idOrName);
        if (config == null ) {
            config = getConfigByName(cls, idOrName);
        }
        return ofNullable(config);
    }

    public List<RegistryConfig> getDefaultRegistries() {
        return getDefaultConfigs(getConfigsMap(getTagName(RegistryConfig.class)));
    }

    public Collection<RegistryConfig> getRegistries() {
        return getConfigs(getTagName(RegistryConfig.class));
    }

    // ServiceConfig correlative methods

    public void addService(ServiceConfigBase<?> serviceConfig) {
        addConfig(serviceConfig);
    }

    public void addServices(Iterable<ServiceConfigBase<?>> serviceConfigs) {
        serviceConfigs.forEach(this::addService);
    }

    public Collection<ServiceConfigBase> getServices() {
        return getConfigs(getTagName(ServiceConfigBase.class));
    }

    public <T> ServiceConfigBase<T> getService(String id) {
        return getConfig(ServiceConfigBase.class, id).orElse(null);
    }

    // ReferenceConfig correlative methods

    public void addReference(ReferenceConfigBase<?> referenceConfig) {
        addConfig(referenceConfig);
    }

    public void addReferences(Iterable<ReferenceConfigBase<?>> referenceConfigs) {
        referenceConfigs.forEach(this::addReference);
    }

    public Collection<ReferenceConfigBase<?>> getReferences() {
        return getConfigs(getTagName(ReferenceConfigBase.class));
    }

    public <T> ReferenceConfigBase<T> getReference(String id) {
        return getConfig(ReferenceConfigBase.class, id).orElse(null);
    }

    public void refreshAll() {
        // refresh all configs here,
        getApplication().ifPresent(ApplicationConfig::refresh);
        getMonitor().ifPresent(MonitorConfig::refresh);
        getModule().ifPresent(ModuleConfig::refresh);
        getMetrics().ifPresent(MetricsConfig::refresh);
        getSsl().ifPresent(SslConfig::refresh);

        getProtocols().forEach(ProtocolConfig::refresh);
        getRegistries().forEach(RegistryConfig::refresh);
        getProviders().forEach(ProviderConfig::refresh);
        getConsumers().forEach(ConsumerConfig::refresh);
        getConfigCenters().forEach(ConfigCenterConfig::refresh);
        getMetadataConfigs().forEach(MetadataReportConfig::refresh);
    }

    /**
     * In some scenario,  we may nee to add and remove ServiceConfig or ReferenceConfig dynamically.
     *
     * @param config the config instance to remove.
     */
    public void removeConfig(AbstractConfig config) {
        if (config == null) {
            return;
        }

        Map<String, AbstractConfig> configs = configsCache.get(getTagName(config.getClass()));
        if (CollectionUtils.isNotEmptyMap(configs)) {
            configs.values().removeIf(c -> config == c);
        }
    }

    public void clear() {
        this.configsCache.clear();
        configIdIndexes.clear();
        this.referenceConfigCache.clear();
        this.serviceConfigCache.clear();
        this.duplicatedConfigs.clear();
    }

    /**
     * @throws IllegalStateException
     * @since 2.7.8
     */
    @Override
    public void destroy() throws IllegalStateException {
        clear();
    }

    /**
     * Add the dubbo {@link AbstractConfig config}
     *
     * @param config the dubbo {@link AbstractConfig config}
     */
    public void addConfig(AbstractConfig config) {
        if (config == null) {
            return;
        }
        addConfig(config, isUniqueConfig(config));
    }

    private boolean isUniqueConfig(AbstractConfig config) {
        return uniqueConfigTypes.contains(config.getClass());
    }

    protected <T extends AbstractConfig> T addConfig(AbstractConfig config, boolean unique) {
        if (config == null) {
            return null;
        }
        // ignore MethodConfig
        if (config instanceof MethodConfig) {
            return null;
        }

        Map<String, AbstractConfig> configsMap = configsCache.computeIfAbsent(getTagName(config.getClass()), type -> newMap());

        // fast check duplicated equivalent config before write lock
        if (!(config instanceof ReferenceConfigBase || config instanceof ServiceConfigBase)) {
            for (AbstractConfig value : configsMap.values()) {
                if (value.equals(config)) {
                    return (T) value;
                }
            }
        }

        // lock by config type
        synchronized (configsMap) {
            return (T) addIfAbsent(config, configsMap, unique);
        }
    }

    public <C extends AbstractConfig> Map<String, C> getConfigsMap(Class<C> cls) {
        return getConfigsMap(getTagName(cls));
    }

    private <C extends AbstractConfig> Map<String, C> getConfigsMap(String configType) {
        return (Map<String, C>) configsCache.getOrDefault(configType, emptyMap());
    }

    private <C extends AbstractConfig> Collection<C> getConfigs(String configType) {
        return (Collection<C>) getConfigsMap(configType).values();
    }

    public <C extends AbstractConfig> Collection<C> getConfigs(Class<C> configType) {
        return (Collection<C>) getConfigsMap(getTagName(configType)).values();
    }

    /**
     * Get config by id
     * @param configType
     * @param id
     * @return
     */
    private <C extends AbstractConfig> C getConfigById(String configType, String id) {
        return (C) getConfigsMap(configType).get(id);
    }

    /**
     * Get config by name if existed
     * @param cls
     * @param name
     * @return
     */
    private <C extends AbstractConfig> C getConfigByName(Class<? extends C> cls, String name) {
        Map<String, ? extends C> configsMap = getConfigsMap(cls);
        if (configsMap.isEmpty()) {
            return null;
        }
        // try find config by name
        if (ReflectUtils.hasMethod(cls, CONFIG_NAME_READ_METHOD)) {
            List<C> list = configsMap.values().stream()
                .filter(cfg -> name.equals(getConfigName(cfg)))
                .collect(Collectors.toList());
            if (list.size() > 1) {
                throw new IllegalStateException("Found more than one config by name: " + name +
                    ", instances: " + list + ". Please remove redundant configs or get config by id.");
            } else if (list.size() == 1) {
                return list.get(0);
            }
        }
        return null;
    }

    private <C extends AbstractConfig> String getConfigName(C config) {
        try {
            return (String) ReflectUtils.getProperty(config, CONFIG_NAME_READ_METHOD);
        } catch (Exception e) {
            return null;
        }
    }

    protected <C extends AbstractConfig> C getSingleConfig(String configType) throws IllegalStateException {
        Map<String, AbstractConfig> configsMap = getConfigsMap(configType);
        int size = configsMap.size();
        if (size < 1) {
//                throw new IllegalStateException("No such " + configType.getName() + " is found");
            return null;
        } else if (size > 1) {
            throw new IllegalStateException("Expected single instance of " + configType + ", but found " + size +
                " instances, please remove redundant configs. instances: "+configsMap.values());
        }
        return (C) configsMap.values().iterator().next();
    }

    private static boolean isEquals(AbstractConfig oldOne, AbstractConfig newOne) {
        if (oldOne == newOne) {
            return true;
        }
        if (oldOne == null || newOne == null) {
            return false;
        }
        if (oldOne.getClass() != newOne.getClass()) {
            return false;
        }
        // make both are refreshed or none is refreshed
        if (oldOne.isRefreshed() || newOne.isRefreshed()) {
            if (!oldOne.isRefreshed()) {
                oldOne.refresh();
            }
            if (!newOne.isRefreshed()) {
                newOne.refresh();
            }
        }
        return oldOne.equals(newOne);
    }

    private static Map newMap() {
        return new ConcurrentHashMap();
    }

    /**
     * Add config
     * @param config
     * @param configsMap
     * @param unique
     * @return the existing equivalent config or the new adding config
     * @throws IllegalStateException
     */
    private <C extends AbstractConfig> C addIfAbsent(C config, Map<String, C> configsMap, boolean unique)
            throws IllegalStateException {

        if (config == null || configsMap == null) {
            return config;
        }

        // check duplicated configs
        // special check service and reference config by unique service name, speed up the processing of large number of instances
        if (config instanceof ReferenceConfigBase || config instanceof ServiceConfigBase) {
            C existedConfig = (C) checkDuplicatedInterfaceConfig((AbstractInterfaceConfig) config);
            if (existedConfig != null) {
                return existedConfig;
            }
        } else {
            // find by value
            Optional<C> prevConfig = findConfigByValue(configsMap.values(), config);
            if (prevConfig.isPresent()) {
                if (prevConfig.get() == config) {
                    // the new one is same as existing one
                    return prevConfig.get();
                }

                // ignore duplicated equivalent config
                if (logger.isInfoEnabled() && duplicatedConfigs.add(config)) {
                    logger.info("Ignore duplicated config: " + config);
                }
                return prevConfig.get();
            }
        }

        // check unique config
        if (unique && configsMap.size() > 0) {
            C oldOne = configsMap.values().iterator().next();
            String configName = oldOne.getClass().getSimpleName();
            String msgPrefix = "Duplicate Configs found for " + configName + ", only one unique " + configName +
                " is allowed for one application. previous: " + oldOne + ", later: " + config + ". According to config mode [" + configMode + "], ";
            switch (configMode) {
                case STRICT: {
                    if (!isEquals(oldOne, config)) {
                        throw new IllegalStateException(msgPrefix + "please remove redundant configs and keep only one.");
                    }
                    break;
                }
                case IGNORE: {
                    // ignore later config
                    if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                        logger.warn(msgPrefix + "keep previous config and ignore later config");
                    }
                    return oldOne;
                }
                case OVERRIDE: {
                    // clear previous config, add new config
                    configsMap.clear();
                    if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                        logger.warn(msgPrefix + "override previous config with later config");
                    }
                    break;
                }
            }
        }

        String key = getId(config);
        if (key == null) {
            do {
                // generate key if id is not set
                key = generateConfigId(config);
            } while (configsMap.containsKey(key));
        }

        C existedConfig = configsMap.get(key);
        if (existedConfig != null && !isEquals(existedConfig, config)) {
            String type = config.getClass().getSimpleName();
            logger.warn(String.format("Duplicate %s found, there already has one default %s or more than two %ss have the same id, " +
                    "you can try to give each %s a different id, override previous config with later config. id: %s, prev: %s, later: %s",
                type, type, type, type, key, existedConfig, config));
        }

        // override existed config if any
        configsMap.put(key, config);
        return config;
    }

    private <C extends AbstractConfig> Optional<C> findConfigByValue(Collection<C> values, C config) {
        // 1. find same config instance (speed up raw api usage)
        Optional<C> prevConfig = values.stream().filter(val -> val == config).findFirst();
        if (prevConfig.isPresent()) {
            return prevConfig;
        }

        // 2. find equal config
        prevConfig = values.stream()
            .filter(val -> isEquals(val, config))
            .findFirst();
        return prevConfig;
    }

    /**
     * check duplicated ReferenceConfig/ServiceConfig
     * @param config
     */
    private AbstractInterfaceConfig checkDuplicatedInterfaceConfig(AbstractInterfaceConfig config) {
        String uniqueServiceName;
        Map<String, AbstractInterfaceConfig> configCache;
        if (config instanceof ReferenceConfigBase) {
            ReferenceConfigBase<?> referenceConfig = (ReferenceConfigBase<?>) config;
            uniqueServiceName = referenceConfig.getUniqueServiceName();
            configCache = referenceConfigCache;
        } else if (config instanceof ServiceConfigBase) {
            ServiceConfigBase serviceConfig = (ServiceConfigBase) config;
            uniqueServiceName = serviceConfig.getUniqueServiceName();
            configCache = serviceConfigCache;
        } else {
            throw new IllegalArgumentException("Illegal type of parameter 'config' : " + config.getClass().getName());
        }

        AbstractInterfaceConfig prevConfig = configCache.putIfAbsent(uniqueServiceName, config);
        if (prevConfig != null) {
            if (prevConfig == config) {
                return prevConfig;
            }

            if (prevConfig.equals(config)) {
                // TODO Is there any problem with ignoring duplicate and equivalent but different ReferenceConfig instances?
                if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                    logger.warn("Ignore duplicated and equal config: "+config);
                }
                return prevConfig;
            }

            String configType = config.getClass().getSimpleName();
            String msg = "Found multiple " + configType + "s with unique service name [" +
                uniqueServiceName + "], previous: " + prevConfig + ", later: " + config + ". " +
                "There can only be one instance of " + configType + " with the same triple (group, interface, version). " +
                "If multiple instances are required for the same interface, please use a different group or version.";

            if (logger.isWarnEnabled() && duplicatedConfigs.add(config)) {
                logger.warn(msg);
            }
            if (!ignoreDuplicatedInterface) {
                throw new IllegalStateException(msg);
            }
        }
        return prevConfig;
    }

    public static <C extends AbstractConfig> String generateConfigId(C config) {
        String tagName = getTagName(config.getClass());
        int idx = configIdIndexes.computeIfAbsent(tagName, clazz -> new AtomicInteger(0)).incrementAndGet();
        return tagName + "#" + idx;
    }

    static <C extends AbstractConfig> String getId(C config) {
        String id = config.getId();
        return isNotEmpty(id) ? id : null;
    }

    static <C extends AbstractConfig> Boolean isDefaultConfig(C config) {
        return config.isDefault();
    }

    static <C extends AbstractConfig> List<C> getDefaultConfigs(Map<String, C> configsMap) {
        // find isDefault() == true
        List<C> list = configsMap.values()
                .stream()
                .filter(c -> TRUE.equals(ConfigManager.isDefaultConfig(c)))
                .collect(Collectors.toList());
        if (list.size() > 0) {
            return list;
        }

        // find isDefault() == null
        list = configsMap.values()
                .stream()
                .filter(c -> ConfigManager.isDefaultConfig(c) == null)
                .collect(Collectors.toList());
        return list;

        // exclude isDefault() == false
    }

    protected ConfigMode getConfigMode() {
        return configMode;
    }
}
