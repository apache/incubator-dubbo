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
package org.apache.dubbo.config.spring.issues;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.*;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The test-case for https://github.com/apache/dubbo/issues/7752
 *
 * @since 2.7.8
 */
@Configuration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class Issue7752Test {

    /**
     * code is same with org.springframework.context.support.PostProcessorRegistrationDelegate$BeanPostProcessorChecker
     */
    private static final class Issue7752TestBeanPostProcessorChecker implements BeanPostProcessor{

        private ConfigurableListableBeanFactory beanFactory;

        private int beanPostProcessorTargetCount;

        public Issue7752TestBeanPostProcessorChecker(ConfigurableListableBeanFactory beanFactory, int beanPostProcessorTargetCount){
            this.beanFactory = beanFactory;
            this.beanPostProcessorTargetCount = beanPostProcessorTargetCount;
        }

        @Override
        public Object postProcessBeforeInitialization(Object bean, String beanName) {
            return bean;
        }

        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (!(bean instanceof BeanPostProcessor) && !isInfrastructureBean(beanName) &&
                    this.beanFactory.getBeanPostProcessorCount() < this.beanPostProcessorTargetCount) {
                //means may be some BeanPostProcessor not deal with bean
                Assertions.assertTrue(false);
            }
            return bean;
        }

        private boolean isInfrastructureBean(@Nullable String beanName) {
            if (beanName != null && this.beanFactory.containsBeanDefinition(beanName)) {
                BeanDefinition bd = this.beanFactory.getBeanDefinition(beanName);
                return (bd.getRole() == RootBeanDefinition.ROLE_INFRASTRUCTURE);
            }
            return false;
        }
    }

    @Test
    public void test() throws Exception {
        //for replace map
        Map<Object, Issue7752TestBeanPostProcessorChecker> map = new ConcurrentHashMap();

        //mock Construction
        Class<?> testCheck = Class.forName("org.springframework.context.support.PostProcessorRegistrationDelegate$BeanPostProcessorChecker");
        Mockito.mockConstruction(testCheck, Mockito.withSettings().defaultAnswer(invocation -> {
            //call same method
            return Issue7752TestBeanPostProcessorChecker.class.getMethod(invocation.getMethod().getName(), invocation.getMethod().getParameterTypes())
                    .invoke(map.get(invocation.getMock()), invocation.getArguments());
        }), (MockedConstruction.MockInitializer) (mock, context) -> {
            //create replace bean process
            map.put(mock, Issue7752TestBeanPostProcessorChecker.class.getConstructor(ConfigurableListableBeanFactory.class, int.class).newInstance(context.arguments().toArray()));
        });

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Issue7752Test.class);
        context.close();
    }

}
