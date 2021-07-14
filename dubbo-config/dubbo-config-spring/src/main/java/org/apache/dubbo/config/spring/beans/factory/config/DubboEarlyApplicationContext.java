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
package org.apache.dubbo.config.spring.beans.factory.config;

import com.alibaba.spring.beans.factory.config.GenericBeanPostProcessorAdapter;
import org.apache.dubbo.config.AbstractConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.CommonAnnotationBeanPostProcessor;
import org.springframework.core.PriorityOrdered;

import javax.annotation.PostConstruct;

/**
 * Generally, {@link AbstractConfig Dubbo Config} Bean will be added into {@link ConfigManager} on the bean initialization
 * life cycle through {@link CommonAnnotationBeanPostProcessor} executing the callback of
 * {@link PostConstruct @PostConstruct}. However, the instantiation and initialization of
 * {@link AbstractConfig Dubbo Config} Bean could be too early before {@link CommonAnnotationBeanPostProcessor}, e.g,
 * execution, thus it's required to register the current instance as a {@link BeanPostProcessor} into
 * {@link DefaultListableBeanFactory the BeanFatory} using {@link BeanDefinitionRegistryPostProcessor} as early as
 * possible.
 *
 * @see GenericBeanPostProcessorAdapter
 * @since 2.7.9
 */
public class DubboEarlyApplicationContext implements BeanDefinitionRegistryPostProcessor, PriorityOrdered {

    public static final String BEAN_NAME = "dubboEarlyApplicationContext";

    private DefaultListableBeanFactory beanFactory;

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        this.beanFactory = unwrap(registry);
        this.beanFactory.addBeanPostProcessor(new DubboConfigEarlyInitializationPostProcessor(this.beanFactory));
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        if (beanFactory == null) { // try again if postProcessBeanDefinitionRegistry method does not effect.
            this.beanFactory = unwrap(beanFactory);
            this.beanFactory.addBeanPostProcessor(new DubboConfigEarlyInitializationPostProcessor(this.beanFactory));
        }
    }

    private DefaultListableBeanFactory unwrap(Object registry) {
        if (registry instanceof DefaultListableBeanFactory) {
            return (DefaultListableBeanFactory) registry;
        }
        return null;
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
