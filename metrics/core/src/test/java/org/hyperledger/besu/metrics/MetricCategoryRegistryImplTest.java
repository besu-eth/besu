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
package org.hyperledger.besu.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class MetricCategoryRegistryImplTest {

  @Test
  void metricCategoryIsEnabledAndMetricsAreEnabled() {
    final var registry = new MetricCategoryRegistryImpl();
    registry.addMetricCategory(BesuMetricCategory.BLOCKCHAIN);
    final var metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(true)
            .metricCategories(Set.of(BesuMetricCategory.BLOCKCHAIN))
            .build();
    registry.setMetricsConfiguration(metricsConfiguration);
    assertTrue(registry.isMetricCategoryEnabled(BesuMetricCategory.BLOCKCHAIN));
  }

  @Test
  void metricCategoryIsDisabledAndMetricsAreEnabled() {
    final var registry = new MetricCategoryRegistryImpl();
    registry.addMetricCategory(BesuMetricCategory.ETHEREUM);
    final var metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(true)
            .metricCategories(Set.of(BesuMetricCategory.ETHEREUM))
            .build();
    registry.setMetricsConfiguration(metricsConfiguration);
    assertFalse(registry.isMetricCategoryEnabled(BesuMetricCategory.BLOCKCHAIN));
  }

  @Test
  void metricCategoryNotEnabledWhenMetricsAreDisabled() {
    final var registry = new MetricCategoryRegistryImpl();
    registry.addMetricCategory(BesuMetricCategory.BLOCKCHAIN);
    final var metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(false)
            .metricCategories(Set.of(BesuMetricCategory.BLOCKCHAIN))
            .build();
    registry.setMetricsConfiguration(metricsConfiguration);
    assertFalse(registry.isMetricCategoryEnabled(BesuMetricCategory.BLOCKCHAIN));
  }

  /** A plugin's category, carrying only its name. */
  private record PluginCategory(String name) implements MetricCategory {
    @Override
    public String getName() {
      return name;
    }

    @Override
    public Optional<String> getApplicationPrefix() {
      return Optional.empty();
    }
  }

  @Test
  void aCategoryNameAnotherPluginRegisteredIsRejectedNamingThatPlugin() {
    final AtomicReference<String> registrant = new AtomicReference<>("pluginA");
    final var registry =
        new MetricCategoryRegistryImpl(() -> true, () -> Optional.ofNullable(registrant.get()));
    registry.addMetricCategory(new PluginCategory("custom"));
    registrant.set("pluginB");

    assertThatThrownBy(() -> registry.addMetricCategory(new PluginCategory("custom")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("cannot register metric category custom: plugin pluginA already registered it");
    assertThat(registry.containsMetricCategory("custom")).isTrue();
  }

  @Test
  void aPluginMayRegisterItsOwnCategoryNameAgain() {
    final var registry = new MetricCategoryRegistryImpl(() -> true, () -> Optional.of("pluginA"));
    final PluginCategory second = new PluginCategory("custom");
    registry.addMetricCategory(new PluginCategory("custom"));

    registry.addMetricCategory(second);

    assertThat(registry.getMetricCategory("custom")).isSameAs(second);
  }

  @Test
  void aPluginCategoryNamedLikeOneOfBesusOwnIsRejectedNamingBesu() {
    final var registry = new MetricCategoryRegistryImpl(() -> true, () -> Optional.of("pluginA"));
    registry.addCategories(StandardMetricCategory.class);

    assertThatThrownBy(() -> registry.addMetricCategory(new PluginCategory("process")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("cannot register metric category process: Besu already registered it");
    assertThat(registry.getMetricCategory("process")).isSameAs(StandardMetricCategory.PROCESS);
  }
}
