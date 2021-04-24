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
package org.apache.dubbo.metadata.store.failover;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportFactory;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReportFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class FailoverMetadataReportTest {

    private ExtensionLoader<MetadataReportFactory> reportLoader = ExtensionLoader.getExtensionLoader(MetadataReportFactory.class);

    private URL mockURL = URL.valueOf("failover://127.0.0.1:2181?clusters=localhost:3181&protocol=mock");

    @AfterEach
    void tearDown() {
        clearFailoverReport();
        clearFailoverFactory();
    }

    @Test
    public void testReadWriteAllMetadataReport() {
        URL url = mockURL.addParameter("strategy", "all");
        FailoverMetadataReport report = getFailoverReport(url);
        Assertions.assertNotNull(report.getProxyReports(), "metadata reports should not be null.");
        Assertions.assertEquals(2, report.getProxyReports().size(),
                "expect 2 metadata report, actual " + report.getProxyReports().size());

        MetadataIdentifier identifier = new MetadataIdentifier("helloService", null, null, null, "test");
        ServiceDefinition definition = new ServiceDefinition();
        definition.setCanonicalName("helloService");
        report.storeProviderMetadata(identifier, definition);
        Assertions.assertNotNull(report.getServiceDefinition(identifier));
        // assert all metadata report write already.
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            Assertions.assertNotNull(holder.report.getServiceDefinition(identifier));
        }

        SubscriberMetadataIdentifier subscribeIdentifier = new SubscriberMetadataIdentifier("test", "1.0");
        MetadataInfo metadataInfo = new MetadataInfo(subscribeIdentifier.getApplication(), subscribeIdentifier.getRevision(), null);
        report.publishAppMetadata(subscribeIdentifier, metadataInfo);
        Assertions.assertEquals(metadataInfo, report.getAppMetadata(subscribeIdentifier, null));
        // assert all metadata report write already.
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            Assertions.assertEquals(metadataInfo, holder.report.getAppMetadata(subscribeIdentifier, null));
        }

        report.registerServiceAppMapping("helloService", "test", null);
        Set<String> appNames = report.getServiceAppMapping("helloService", null, null);
        Assertions.assertEquals(appNames, report.getServiceAppMapping("helloService", null, null));
        // assert all metadata report write already.
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            Assertions.assertEquals(appNames, holder.report.getServiceAppMapping("helloService", null, null));
        }

        ServiceMetadataIdentifier serviceIdentifier = new ServiceMetadataIdentifier("helloService", null, null, null, "1.0", "dubbo");
        report.saveServiceMetadata(serviceIdentifier, url);
        Assertions.assertNotNull(report.getExportedURLs(serviceIdentifier));
        // assert all metadata report write already.
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            Assertions.assertNotNull(holder.report.getExportedURLs(serviceIdentifier));
        }

        report.saveSubscribedData(subscribeIdentifier, new HashSet<>());
        Assertions.assertNotNull(report.getSubscribedURLs(subscribeIdentifier));
        // assert all metadata report write already.
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            Assertions.assertNotNull(holder.report.getSubscribedURLs(subscribeIdentifier));
        }
    }

    @Test
    public void testLocalDataCenterMetadataReport() {
        URL url = mockURL.addParameter("strategy", "local");
        FailoverMetadataReport report = getFailoverReport(url);
        Assertions.assertNotNull(report.getProxyReports(), "metadata reports should not be null.");
        Assertions.assertEquals(2, report.getProxyReports().size(),
                "expect 2 metadata report, actual " + report.getProxyReports().size());

        MetadataReport localReport = null, failoverReport = null;
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            if (holder.url.getBackupAddress().contains(url.getAddress())) {
                localReport = holder.report;
            } else {
                failoverReport = holder.report;
            }
        }
        Assertions.assertNotNull(localReport);
        Assertions.assertNotNull(failoverReport);

        MetadataIdentifier identifier = new MetadataIdentifier("helloService", null, null, null, "test");
        ServiceDefinition definition = new ServiceDefinition();
        definition.setCanonicalName("helloService");
        report.storeProviderMetadata(identifier, definition);

        // assert local metadata report write already.
        Assertions.assertNotNull(report.getServiceDefinition(identifier));
        Assertions.assertNotNull(localReport.getServiceDefinition(identifier));
        Assertions.assertNull(failoverReport.getServiceDefinition(identifier));

        SubscriberMetadataIdentifier subscribeIdentifier = new SubscriberMetadataIdentifier("test", "1.0");
        MetadataInfo metadataInfo = new MetadataInfo(subscribeIdentifier.getApplication(), subscribeIdentifier.getRevision(), null);
        report.publishAppMetadata(subscribeIdentifier, metadataInfo);
        // assert all metadata report write already.
        Assertions.assertEquals(metadataInfo, report.getAppMetadata(subscribeIdentifier, null));
        Assertions.assertEquals(metadataInfo, localReport.getAppMetadata(subscribeIdentifier, null));
        Assertions.assertNotEquals(metadataInfo, failoverReport.getAppMetadata(subscribeIdentifier, null));

        report.registerServiceAppMapping("helloService", "test", null);
        Set<String> appNames = report.getServiceAppMapping("helloService", null, null);

        // assert local metadata report write already.
        Assertions.assertEquals(appNames, report.getServiceAppMapping("helloService", null, null));
        Assertions.assertEquals(appNames, localReport.getServiceAppMapping("helloService", null, null));
        Assertions.assertNotEquals(appNames, failoverReport.getServiceAppMapping("helloService", null, null));

        ServiceMetadataIdentifier serviceIdentifier = new ServiceMetadataIdentifier("helloService", null, null, null, "1.0", "dubbo");
        report.saveServiceMetadata(serviceIdentifier, url);
        // assert local metadata report write already.
        Assertions.assertNotNull(report.getExportedURLs(serviceIdentifier));
        Assertions.assertNotNull(localReport.getExportedURLs(serviceIdentifier));
        Assertions.assertNull(failoverReport.getExportedURLs(serviceIdentifier));

        Set<String> urls = new HashSet<>();
        urls.add(url.toFullString());
        report.saveSubscribedData(subscribeIdentifier, urls);
        // assert local metadata report write already.
        Assertions.assertEquals(new ArrayList<>(urls), report.getSubscribedURLs(subscribeIdentifier));
        Assertions.assertEquals(new ArrayList<>(urls), localReport.getSubscribedURLs(subscribeIdentifier));
        Assertions.assertNotEquals(new ArrayList<>(urls), failoverReport.getSubscribedURLs(subscribeIdentifier));
    }

    protected FailoverMetadataReport getFailoverReport(URL url) {
        MetadataReportFactory reportFactory = reportLoader.getExtension(url.getProtocol());
        Assertions.assertTrue(reportFactory instanceof FailoverMetadataReportFactory,
                "expect " + FailoverMetadataReportFactory.class.getName() + " instance type, "
                        + "actual " + reportFactory.getClass().getName() + " instance type");

        MetadataReport report = reportFactory.getMetadataReport(url);
        Assertions.assertTrue(report instanceof FailoverMetadataReport,
                "expect " + FailoverMetadataReport.class.getName() + " instance type, "
                        + "actual " + report.getClass().getName() + " instance type");

        FailoverMetadataReport failover = (FailoverMetadataReport) report;
        return failover;
    }

    private void clearFailoverReport() {
        FailoverMetadataReport report = getFailoverReport(mockURL);
        for (FailoverMetadataReport.MetadataReportHolder holder : report.getProxyReports()) {
            if (holder.report instanceof MockMetadataReport) {
                ((MockMetadataReport) (holder.report)).reset();
            }
        }
    }

    private void clearFailoverFactory() {
        MetadataReportFactory factory = reportLoader.getExtension(mockURL.getProtocol());
        try {
            Field reportCache = AbstractMetadataReportFactory.class.getDeclaredField("SERVICE_STORE_MAP");
            if (!reportCache.isAccessible()) {
                ReflectUtils.makeAccessible(reportCache);
            }
            Map<String, MetadataReport> serviceStore = (Map<String, MetadataReport>) reportCache.get(factory);
            if (serviceStore != null) {
                for (Iterator<Map.Entry<String, MetadataReport>> iterator = serviceStore.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry<String, MetadataReport> entry = iterator.next();
                    if (entry.getKey().startsWith(mockURL.getProtocol())) {
                        iterator.remove();
                    }
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
        }
    }

}
