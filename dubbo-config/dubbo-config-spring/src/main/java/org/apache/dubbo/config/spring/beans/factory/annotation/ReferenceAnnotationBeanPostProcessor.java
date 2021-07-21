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
package org.apache.dubbo.config.spring.beans.factory.annotation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.spring.Constants;
import org.apache.dubbo.config.spring.context.event.DubboAnnotationInitedEvent;
import org.apache.dubbo.config.spring.reference.ReferenceAttributes;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.reference.ReferenceBeanManager;
import org.apache.dubbo.config.spring.reference.ReferenceBeanSupport;
import org.apache.dubbo.rpc.service.GenericService;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.InjectionMetadata;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.ScannedGenericBeanDefinition;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.MethodMetadata;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Member;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.spring.util.AnnotationUtils.getAttribute;
import static org.apache.dubbo.common.utils.AnnotationUtils.filterDefaultValues;
import static org.springframework.util.StringUtils.hasText;

/**
 * <p>
 * Step 1:
 * The purpose of implementing {@link BeanFactoryPostProcessor} is to scan the registration reference bean definition earlier,
 * so that it can be shared with the xml bean configuration.
 * </p>
 *
 * <p>
 * Step 2:
 * By implementing {@link org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor},
 * inject the reference bean instance into the fields and setter methods which annotated with {@link DubboReference}.
 * </p>
 *
 * @see DubboReference
 * @see Reference
 * @see com.alibaba.dubbo.config.annotation.Reference
 * @since 2.5.7
 */
public class ReferenceAnnotationBeanPostProcessor extends AbstractAnnotationBeanPostProcessor
        implements ApplicationContextAware, BeanFactoryPostProcessor {

    /**
     * The bean name of {@link ReferenceAnnotationBeanPostProcessor}
     */
    public static final String BEAN_NAME = "referenceAnnotationBeanPostProcessor";

    /**
     * Cache size
     */
    private static final int CACHE_SIZE = Integer.getInteger(BEAN_NAME + ".cache.size", 32);

    private final Log logger = LogFactory.getLog(getClass());

    private final ConcurrentMap<InjectionMetadata.InjectedElement, String> injectedFieldReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private final ConcurrentMap<InjectionMetadata.InjectedElement, String> injectedMethodReferenceBeanCache =
            new ConcurrentHashMap<>(CACHE_SIZE);

    private ApplicationContext applicationContext;

    private ReferenceBeanManager referenceBeanManager;
    private BeanDefinitionRegistry beanDefinitionRegistry;

    /**
     * {@link com.alibaba.dubbo.config.annotation.Reference @com.alibaba.dubbo.config.annotation.Reference} has been supported since 2.7.3
     * <p>
     * {@link DubboReference @DubboReference} has been supported since 2.7.7
     */
    public ReferenceAnnotationBeanPostProcessor() {
        super(DubboReference.class, Reference.class, com.alibaba.dubbo.config.annotation.Reference.class);
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

        String[] beanNames = beanFactory.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Class<?> beanType;
            if (beanFactory.isFactoryBean(beanName)){
                BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
                if (isReferenceBean(beanDefinition)) {
                    continue;
                }
                if (isAnnotatedReferenceBean(beanDefinition)) {
                    // process @DubboReference at java-config @bean method
                    processReferenceAnnotatedBeanDefinition(beanName, (AnnotatedBeanDefinition) beanDefinition);
                    continue;
                }

                String beanClassName = beanDefinition.getBeanClassName();
//                if (beanDefinition instanceof AnnotatedBeanDefinition) {
//                    AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition) beanDefinition;
//                    beanClassName = annotatedBeanDefinition.getFactoryMethodMetadata().getDeclaringClassName();
//                }
                beanType = ClassUtils.resolveClass(beanClassName, getClassLoader());
            } else {
                beanType = beanFactory.getType(beanName);
            }
            if (beanType != null) {
                AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, beanType, null);
                try {
                    prepareInjection(metadata);
                } catch (BeansException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException("Prepare dubbo reference injection element failed", e);
                }
            }
        }

        // This bean has bean register as BeanPostProcessor at org.apache.dubbo.config.spring.context.DubboInfraBeanRegisterPostProcessor.postProcessBeanFactory()
        // so destroy this bean here, prevent register it as BeanPostProcessor again, avoid cause BeanPostProcessorChecker detection error
        beanDefinitionRegistry.removeBeanDefinition(BEAN_NAME);

        // this is an early event, it will be notified at org.springframework.context.support.AbstractApplicationContext.registerListeners()
        applicationContext.publishEvent(new DubboAnnotationInitedEvent(applicationContext));
    }

    /**
     * check whether is @DubboReference at java-config @bean method
     */
    private boolean isAnnotatedReferenceBean(BeanDefinition beanDefinition) {
        if (beanDefinition instanceof AnnotatedBeanDefinition) {
            AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition)beanDefinition;
            if (annotatedBeanDefinition.getFactoryMethodMetadata() == null) {
                return false;
            }
            if (!(annotatedBeanDefinition instanceof ScannedGenericBeanDefinition)) {
                String beanClassName = annotatedBeanDefinition.getFactoryMethodMetadata().getReturnTypeName();
                if (StringUtils.isEquals(ReferenceBean.class.getName(), beanClassName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * process @DubboReference at java-config @bean method
     * <pre class="code">
     * &#064;Configuration
     * public class ConsumerConfig {
     *
     *      &#064;Bean
     *      &#064;DubboReference(group="demo", version="1.2.3")
     *      public ReferenceBean&lt;DemoService&gt; demoService() {
     *          return new ReferenceBean();
     *      }
     *
     * }
     * </pre>
     * @param beanName
     * @param beanDefinition
     */
    private void processReferenceAnnotatedBeanDefinition(String beanName, AnnotatedBeanDefinition beanDefinition) {

        // Extract beanClass from generic return type of java-config bean method: ReferenceBean<DemoService>
        // see org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.getTypeForFactoryBeanFromMethod
        Class beanClass = getBeanFactory().getType(beanName);
        MethodMetadata factoryMethodMetadata = beanDefinition.getFactoryMethodMetadata();
        if (beanClass == null) {
            String beanMethodSignature = factoryMethodMetadata.getDeclaringClassName()+"#"+factoryMethodMetadata.getMethodName()+"()";
            throw new BeanCreationException("The ReferenceBean is missing necessary generic type, which returned by the @Bean method of Java-config class. " +
                    "The generic type of the returned ReferenceBean must be specified as the referenced interface type, " +
                    "such as ReferenceBean<DemoService>. Please check bean method: "+beanMethodSignature);
        }

        // get dubbo reference annotation attributes
        Map<String, Object> annotationAttributes = null;
        // try all dubbo reference annotation types
        for (Class<? extends Annotation> annotationType : getAnnotationTypes()) {
            if (factoryMethodMetadata.isAnnotated(annotationType.getName())) {
                // Since Spring 5.2
                // return factoryMethodMetadata.getAnnotations().get(annotationType).filterDefaultValues().asMap();
                // Compatible with Spring 4.x
                annotationAttributes = factoryMethodMetadata.getAnnotationAttributes(annotationType.getName());
                annotationAttributes = filterDefaultValues(annotationType, annotationAttributes);
            }
        }

        if (annotationAttributes != null) {
            // @DubboReference on @Bean method
            LinkedHashMap<String, Object> attributes = new LinkedHashMap<>(annotationAttributes);
            // reset id attribute
            attributes.put(ReferenceAttributes.ID, beanName);
            // convert annotation props
            ReferenceBeanSupport.convertReferenceProps(attributes, beanClass);

            // get interface
            String interfaceName = (String) attributes.get(ReferenceAttributes.INTERFACE);
            // check beanClass and reference interface class
            if (!StringUtils.isEquals(interfaceName, beanClass.getName()) && beanClass != GenericService.class) {
                String beanMethodSignature = factoryMethodMetadata.getDeclaringClassName()+"#"+factoryMethodMetadata.getMethodName()+"()";
                throw new BeanCreationException("The 'interfaceClass' or 'interfaceName' attribute value of @DubboReference annotation " +
                        "is inconsistent with the generic type of the ReferenceBean returned by the bean method. " +
                        "The interface class of @DubboReference is: "+interfaceName+", but return ReferenceBean<"+beanClass.getName()+">. " +
                        "Please remove the 'interfaceClass' and 'interfaceName' attributes from @DubboReference annotation. " +
                        "Please check bean method: "+beanMethodSignature);
            }

            Class interfaceClass = beanClass;

            // set attribute instead of property values
            beanDefinition.setAttribute(Constants.REFERENCE_PROPS, attributes);
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, interfaceClass);
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, interfaceName);
        } else {
            // raw reference bean
            // the ReferenceBean is not yet initialized
            beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, beanClass);
            if (beanClass != GenericService.class) {
                beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, beanClass.getName());
            }
        }

        // set id
        beanDefinition.getPropertyValues().add(ReferenceAttributes.ID, beanName);
    }

    @Override
    public void postProcessMergedBeanDefinition(RootBeanDefinition beanDefinition, Class<?> beanType, String beanName) {
        if (beanType != null) {
            if (isReferenceBean(beanDefinition)) {
                //mark property value as optional
                List<PropertyValue> propertyValues = beanDefinition.getPropertyValues().getPropertyValueList();
                for (PropertyValue propertyValue : propertyValues) {
                    propertyValue.setOptional(true);
                }
            } else if (isAnnotatedReferenceBean(beanDefinition)) {
                // extract beanClass from java-config bean method generic return type: ReferenceBean<DemoService>
                //Class beanClass = getBeanFactory().getType(beanName);
            } else {
                AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, beanType, null);
                metadata.checkConfigMembers(beanDefinition);
                try {
                    prepareInjection(metadata);
                } catch (Exception e) {
                    throw new IllegalStateException("Prepare dubbo reference injection element failed", e);
                }
            }
        }
    }

    @Override
    public PropertyValues postProcessPropertyValues(
            PropertyValues pvs, PropertyDescriptor[] pds, Object bean, String beanName) throws BeansException {

        try {
            AnnotatedInjectionMetadata metadata = findInjectionMetadata(beanName, bean.getClass(), pvs);
            prepareInjection(metadata);
            metadata.inject(bean, beanName, pvs);
        } catch (BeansException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new BeanCreationException(beanName, "Injection of @" + getAnnotationType().getSimpleName()
                    + " dependencies is failed", ex);
        }
        return pvs;
    }

    private boolean isReferenceBean(BeanDefinition beanDefinition) {
        return ReferenceBean.class.getName().equals(beanDefinition.getBeanClassName());
    }

    protected void prepareInjection(AnnotatedInjectionMetadata metadata) throws BeansException {
        try {
            //find and registry bean definition for @DubboReference/@Reference
            for (AnnotatedFieldElement fieldElement : metadata.getFieldElements()) {
                if (fieldElement.injectedObject != null) {
                    continue;
                }
                Class<?> injectedType = fieldElement.field.getType();
                AnnotationAttributes attributes = fieldElement.attributes;
                String referenceBeanName = registerReferenceBean(fieldElement.getPropertyName(), injectedType, attributes, fieldElement.field);

                //associate fieldElement and reference bean
                fieldElement.injectedObject = referenceBeanName;
                injectedFieldReferenceBeanCache.put(fieldElement, referenceBeanName);

            }

            for (AnnotatedMethodElement methodElement : metadata.getMethodElements()) {
                if (methodElement.injectedObject != null) {
                    continue;
                }
                Class<?> injectedType = methodElement.getInjectedType();
                AnnotationAttributes attributes = methodElement.attributes;
                String referenceBeanName = registerReferenceBean(methodElement.getPropertyName(), injectedType, attributes, methodElement.method);

                //associate fieldElement and reference bean
                methodElement.injectedObject = referenceBeanName;
                injectedMethodReferenceBeanCache.put(methodElement, referenceBeanName);
            }
        } catch (ClassNotFoundException e) {
            throw new BeanCreationException("prepare reference annotation failed", e);
        }
    }

    public String registerReferenceBean(String propertyName, Class<?> injectedType, Map<String, Object> attributes, Member member) throws BeansException {

        boolean renameable = true;
        // referenceBeanName
        String referenceBeanName = getAttribute(attributes, ReferenceAttributes.ID);
        if (hasText(referenceBeanName)) {
            renameable = false;
        } else {
            referenceBeanName = propertyName;
        }

        String checkLocation = "Please check " + member.toString();

        // convert annotation props
        ReferenceBeanSupport.convertReferenceProps(attributes, injectedType);

        // get interface
        String interfaceName = (String) attributes.get(ReferenceAttributes.INTERFACE);
        if (StringUtils.isBlank(interfaceName)) {
            throw new BeanCreationException("Need to specify the 'interfaceName' or 'interfaceClass' attribute of '@DubboReference' if enable generic. "+checkLocation);
        }

        // check reference key
        String referenceKey = ReferenceBeanSupport.generateReferenceKey(attributes, applicationContext);

        // find reference bean name by reference key
        List<String> registeredReferenceBeanNames = referenceBeanManager.getBeanNamesByKey(referenceKey);
        if (registeredReferenceBeanNames.size() > 0) {
            // found same name and reference key
            if (registeredReferenceBeanNames.contains(referenceBeanName)) {
                return referenceBeanName;
            }
//            else if (renameable) {
//                // reference key matched, but difference bean name
//                return registeredReferenceBeanNames.get(0);
//            }
            // specify bean name by id attribute, it will register alias later
        }

        //check bean definition
        if (beanDefinitionRegistry.containsBeanDefinition(referenceBeanName)) {
            BeanDefinition prevBeanDefinition = beanDefinitionRegistry.getBeanDefinition(referenceBeanName);
            String prevBeanType = prevBeanDefinition.getBeanClassName();
            String prevBeanDesc = referenceBeanName + "[" + prevBeanType + "]";
            String newBeanDesc = referenceBeanName + "[" + referenceKey + "]";

            if (isReferenceBean(prevBeanDefinition)) {
                //check reference key
                String prevReferenceKey = ReferenceBeanSupport.generateReferenceKey(prevBeanDefinition, applicationContext);
                if (StringUtils.isEquals(prevReferenceKey, referenceKey)) {
                    //found matched dubbo reference bean, ignore register
                    return referenceBeanName;
                }
                //get interfaceName from attribute
                Class prevInterfaceClass = (Class) prevBeanDefinition.getAttribute(ReferenceAttributes.INTERFACE_CLASS);
                Assert.notNull(prevBeanDefinition, "The interface class of ReferenceBean is not initialized");
                prevBeanType = prevInterfaceClass.getName();
                prevBeanDesc = referenceBeanName + "[" + prevReferenceKey + "]";

                //check bean type
                // If two reference beans are the same type and name, then other class injection them by type or name are confusion.
                // Let it go, Spring find candidate bean will check it
//                if (StringUtils.isEquals(prevBeanType, interfaceName)) {
//                    throw new BeanCreationException("Already exists another reference bean with the same bean name and type but difference attributes. " +
//                        "In order to avoid injection confusion later, please modify the name of one of the beans: " +
//                        "prev: " + prevBeanDesc + ", new: " + newBeanDesc+". "+checkLocation);
//                }
            }

            // bean name from attribute 'id' or java-config bean, cannot be renamed
            if (!renameable) {
                throw new BeanCreationException("Already exists another bean definition with the same bean name [" + referenceBeanName + "], " +
                    "but cannot rename the reference bean name (specify the id attribute or java-config bean), " +
                    "please modify the name of one of the beans: " +
                    "prev: " + prevBeanDesc + ", new: " + newBeanDesc + ". " + checkLocation);
            }

            // the prev bean type is different, rename the new reference bean
            int index = 2;
            String newReferenceBeanName = null;
            while (newReferenceBeanName == null || beanDefinitionRegistry.containsBeanDefinition(newReferenceBeanName)) {
                newReferenceBeanName = referenceBeanName + "#" + index;
                index++;
            }
            newBeanDesc = newReferenceBeanName + "[" + referenceKey + "]";

            logger.warn("Already exists another bean definition with the same bean name [" + referenceBeanName + "], " +
                "rename dubbo reference bean to [" + newReferenceBeanName + "]. " +
                "It is recommended to modify the name of one of the beans to avoid injection problems. " +
                "prev: " + prevBeanDesc + ", new: " + newBeanDesc + ". " + checkLocation);
            referenceBeanName = newReferenceBeanName;
        }
        attributes.put(ReferenceAttributes.ID, referenceBeanName);

        // If registered matched reference before, just register alias
        if (registeredReferenceBeanNames.size() > 0) {
            beanDefinitionRegistry.registerAlias(registeredReferenceBeanNames.get(0), referenceBeanName);
            referenceBeanManager.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);
            return referenceBeanName;
        }

        Class interfaceClass = injectedType;

        // TODO Only register one reference bean for same (group, interface, version)

        // Register the reference bean definition to the beanFactory
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClassName(ReferenceBean.class.getName());
        beanDefinition.getPropertyValues().add(ReferenceAttributes.ID, referenceBeanName);

        // set attribute instead of property values
        beanDefinition.setAttribute(Constants.REFERENCE_PROPS, attributes);
        beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_CLASS, interfaceClass);
        beanDefinition.setAttribute(ReferenceAttributes.INTERFACE_NAME, interfaceName);

        // create decorated definition for reference bean, Avoid being instantiated when getting the beanType of ReferenceBean
        // see org.springframework.beans.factory.support.AbstractBeanFactory#getTypeForFactoryBean()
        GenericBeanDefinition targetDefinition = new GenericBeanDefinition();
        targetDefinition.setBeanClass(interfaceClass);
        String id = (String) beanDefinition.getPropertyValues().get(ReferenceAttributes.ID);
        beanDefinition.setDecoratedDefinition(new BeanDefinitionHolder(targetDefinition, id+"_decorated"));

        // signal object type since Spring 5.2
        beanDefinition.setAttribute(Constants.OBJECT_TYPE_ATTRIBUTE, interfaceClass);

        beanDefinitionRegistry.registerBeanDefinition(referenceBeanName, beanDefinition);
        referenceBeanManager.registerReferenceKeyAndBeanName(referenceKey, referenceBeanName);
        logger.info("Register dubbo reference bean: "+referenceBeanName+" = "+referenceKey+" at "+member);
        return referenceBeanName;
    }

    @Override
    protected Object doGetInjectedBean(AnnotationAttributes attributes, Object bean, String beanName, Class<?> injectedType,
                                       AnnotatedInjectElement injectedElement) throws Exception {

        if (injectedElement.injectedObject == null) {
            throw new IllegalStateException("The AnnotatedInjectElement of @DubboReference should be inited before injection");
        }

        return getBeanFactory().getBean((String) injectedElement.injectedObject);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        this.referenceBeanManager = applicationContext.getBean(ReferenceBeanManager.BEAN_NAME, ReferenceBeanManager.class);
        this.beanDefinitionRegistry = (BeanDefinitionRegistry) applicationContext.getAutowireCapableBeanFactory();
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        this.injectedFieldReferenceBeanCache.clear();
        this.injectedMethodReferenceBeanCache.clear();
    }

    /**
     * Gets all beans of {@link ReferenceBean}
     * @deprecated  use {@link ConfigManager#getReferences()} instead
     */
    @Deprecated
    public Collection<ReferenceBean<?>> getReferenceBeans() {
        return Collections.emptyList();
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected field.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedFieldReferenceBeanMap() {
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> map = new HashMap<>();
        for (Map.Entry<InjectionMetadata.InjectedElement, String> entry : injectedFieldReferenceBeanCache.entrySet()) {
            map.put(entry.getKey(), referenceBeanManager.getById(entry.getValue()));
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Get {@link ReferenceBean} {@link Map} in injected method.
     *
     * @return non-null {@link Map}
     * @since 2.5.11
     */
    public Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> getInjectedMethodReferenceBeanMap() {
        Map<InjectionMetadata.InjectedElement, ReferenceBean<?>> map = new HashMap<>();
        for (Map.Entry<InjectionMetadata.InjectedElement, String> entry : injectedMethodReferenceBeanCache.entrySet()) {
            map.put(entry.getKey(), referenceBeanManager.getById(entry.getValue()));
        }
        return Collections.unmodifiableMap(map);
    }
}
