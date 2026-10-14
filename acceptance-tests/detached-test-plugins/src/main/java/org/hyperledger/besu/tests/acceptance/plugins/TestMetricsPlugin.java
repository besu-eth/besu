/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.tests.acceptance.plugins;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;

import java.util.Locale;
import java.util.Optional;

import com.google.auto.service.AutoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(BesuPlugin.class)
public class TestMetricsPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetricsPlugin.class);

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registering TestMetricsPlugin");
    context
        .getBesuService(MetricCategoryRegistry.class)
        .addMetricCategory(TestMetricCategory.TEST_METRIC_CATEGORY);
  }

  @Override
  public void start(final StartContext context) {
    LOG.info("Starting TestMetricsPlugin");
    context
        .getBesuService(MetricsSystem.class)
        .createGauge(
            TestMetricCategory.TEST_METRIC_CATEGORY,
            "test_metric",
            "Returns 1 on success",
            () -> 1.0);
  }

  @Override
  public void stop() {
    LOG.info("Stopping TestMetricsPlugin");
  }

  public enum TestMetricCategory implements MetricCategory {
    TEST_METRIC_CATEGORY;

    @Override
    public String getName() {
      return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public Optional<String> getApplicationPrefix() {
      return Optional.of("plugin_test_");
    }
  }
}
