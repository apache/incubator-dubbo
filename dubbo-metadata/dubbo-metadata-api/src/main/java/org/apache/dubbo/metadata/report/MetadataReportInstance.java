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
package org.apache.dubbo.metadata.report;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_DIRECTORY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.metadata.report.support.Constants.METADATA_REPORT_KEY;

/**
 * 2019-08-09
 */
public class MetadataReportInstance {

    private static AtomicBoolean init = new AtomicBoolean(false);

    private static final Map<String, MetadataReport> metadataReports = new HashMap<>();

    public static void init(MetadataReportConfig config) {
        if (!init.compareAndSet(false, true)) {
            return;
        }
        MetadataReportFactory metadataReportFactory = ExtensionLoader.getExtensionLoader(MetadataReportFactory.class).getAdaptiveExtension();
        URL url = config.toUrl();
        if (METADATA_REPORT_KEY.equals(url.getProtocol())) {
            String protocol = url.getParameter(METADATA_REPORT_KEY, DEFAULT_DIRECTORY);
            url = URLBuilder.from(url)
                    .setProtocol(protocol)
                    .removeParameter(METADATA_REPORT_KEY)
                    .build();
        }
        url = url.addParameterIfAbsent(APPLICATION_KEY, ApplicationModel.getName());
        String relatedRegistryId = config.getRegistry() == null ? DEFAULT_KEY : config.getRegistry();
//        RegistryConfig registryConfig = ApplicationModel.getConfigManager().getRegistry(relatedRegistryId)
//                .orElseThrow(() -> new IllegalStateException("Registry id " + relatedRegistryId + " does not exist."));
        metadataReports.put(relatedRegistryId, metadataReportFactory.getMetadataReport(url));
    }

    public static Map<String, MetadataReport> getMetadataReports(boolean checked) {
        if (checked) {
            checkInit();
        }
        return metadataReports;
    }

    public static MetadataReport getMetadataReport(String registryKey) {
        checkInit();
        MetadataReport metadataReport = metadataReports.get(registryKey);
        if (metadataReport == null) {
            metadataReport = metadataReports.values().iterator().next();
        }
        return metadataReport;
    }


    private static void checkInit() {
        if (!init.get()) {
            throw new IllegalStateException("the metadata report was not inited.");
        }
    }

    // only for unit test
    @Deprecated
    public static void destroy() {
        if (init.get()) {
            metadataReports.clear();
            init.compareAndSet(true, false);
        }
    }
}
