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
package org.apache.dubbo.registry.client.migration;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationRule;
import org.apache.dubbo.rpc.cluster.support.migration.MigrationStep;

public class MigrationRuleHandler<T> {
    private static final Logger logger = LoggerFactory.getLogger(MigrationRuleHandler.class);

    private MigrationInvoker<T> migrationInvoker;

    public MigrationRuleHandler(MigrationInvoker<T> invoker) {
        this.migrationInvoker = invoker;
    }

    private MigrationStep currentStep;

    public void doMigrate(String rawRule) {
        String defaultStep = MigrationRule.getDefaultStep(migrationInvoker.getRegistryUrl());

        MigrationRule rule = MigrationRule.parse(rawRule, defaultStep);

        if (null != currentStep && currentStep.equals(rule.getStep())) {
            if (logger.isInfoEnabled()) {
                logger.info("Migration step is not change. rule.getStep is " + currentStep.name());
            }
            return;
        } else {
            currentStep = rule.getStep();
        }

        migrationInvoker.setMigrationRule(rule);

        if (migrationInvoker.isMigrationMultiRegistry()) {
            if (migrationInvoker.isServiceInvoker()) {
                migrationInvoker.refreshServiceDiscoveryInvoker();
            } else {
                migrationInvoker.refreshInterfaceInvoker();
            }
        } else {
            switch (rule.getStep()) {
                case APPLICATION_FIRST:
                    migrationInvoker.migrateToServiceDiscoveryInvoker(false);
                    break;
                case FORCE_APPLICATION:
                    migrationInvoker.migrateToServiceDiscoveryInvoker(true);
                    break;
                case FORCE_INTERFACE:
                default:
                    migrationInvoker.fallbackToInterfaceInvoker();
            }
        }
    }
}
