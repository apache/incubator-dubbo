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
package org.apache.dubbo.config.spring.context.annotation;

import org.apache.dubbo.config.DubboShutdownHook;
import org.apache.dubbo.config.spring.api.DemoService;
import org.apache.dubbo.config.spring.context.annotation.consumer.ConsumerConfiguration;
import org.apache.dubbo.config.spring.context.annotation.provider.DemoServiceImpl;
import org.apache.dubbo.config.spring.context.annotation.provider.ProviderConfiguration;
import org.apache.dubbo.rpc.model.ApplicationModel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

/**
 * {@link DubboComponentScanRegistrar} Test
 *
 * @since 2.5.8
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DubboComponentScanRegistrarTest {

    private AnnotationConfigApplicationContext providerContext;
    private AnnotationConfigApplicationContext consumerContext;

    @BeforeEach
    public void setUp() {
        ApplicationModel.reset();
        providerContext = new AnnotationConfigApplicationContext();
        consumerContext = new AnnotationConfigApplicationContext();
    }

    @AfterEach
    public void tearDown() {
        DubboShutdownHook dubboShutdownHook = DubboShutdownHook.getDubboShutdownHook();
        dubboShutdownHook.run();
        ApplicationModel.reset();
        providerContext.close();
        consumerContext.close();
    }

    @Test
    public void test() {

        providerContext.register(ProviderConfiguration.class);

        providerContext.refresh();

        DemoService demoService = providerContext.getBean(DemoService.class);

        String value = demoService.sayName("Mercy");

        Assertions.assertEquals("Hello,Mercy", value);

        Class<?> beanClass = AopUtils.getTargetClass(demoService);

        // DemoServiceImpl with @Transactional
        Assertions.assertEquals(DemoServiceImpl.class, beanClass);

        // Test @Transactional is present or not
        Assertions.assertNotNull(findAnnotation(beanClass, Transactional.class));

        consumerContext.register(ConsumerConfiguration.class);

        consumerContext.refresh();

        ConsumerConfiguration consumerConfiguration = consumerContext.getBean(ConsumerConfiguration.class);

        demoService = consumerConfiguration.getDemoService();

        value = demoService.sayName("Mercy");

        Assertions.assertEquals("Hello,Mercy", value);

        ConsumerConfiguration.Child child = consumerContext.getBean(ConsumerConfiguration.Child.class);

        // From Child

        demoService = child.getDemoServiceFromChild();

        Assertions.assertNotNull(demoService);

        value = demoService.sayName("Mercy");

        Assertions.assertEquals("Hello,Mercy", value);

        // From Parent

        demoService = child.getDemoServiceFromParent();

        Assertions.assertNotNull(demoService);

        value = demoService.sayName("Mercy");

        Assertions.assertEquals("Hello,Mercy", value);

        // From Ancestor

        demoService = child.getDemoServiceFromAncestor();

        Assertions.assertNotNull(demoService);

        value = demoService.sayName("Mercy");

        Assertions.assertEquals("Hello,Mercy", value);

    }


}
