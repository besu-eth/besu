/*
 * Copyright 2019 ConsenSys AG.
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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/** The Metric category registry implementation. */
public class MetricCategoryRegistryImpl implements MetricCategoryRegistry {
  private final Map<String, MetricCategory> metricCategories = new HashMap<>();
  private @Nullable MetricsConfiguration metricsConfiguration;

  /** Default constructor */
  public MetricCategoryRegistryImpl() {}

  /**
   * Add Metrics categories.
   *
   * @param <T> the type parameter
   * @param categoryEnum the category enum
   */
  public <T extends Enum<T> & MetricCategory> void addCategories(final Class<T> categoryEnum) {
    EnumSet.allOf(categoryEnum)
        .forEach(category -> metricCategories.put(category.name(), category));
  }

  /**
   * Add registry category.
   *
   * @param metricCategory the metric category
   */
  @Override
  public void addMetricCategory(final MetricCategory metricCategory) {
    metricCategories.put(metricCategory.getName().toUpperCase(Locale.ROOT), metricCategory);
  }

  @Override
  public boolean isMetricCategoryEnabled(final MetricCategory metricCategory) {
    checkNotNull(
        metricsConfiguration, "Metrics configuration must be set before calling this method");
    final MetricsConfiguration configuredMetrics = requireNonNull(metricsConfiguration);
    return (configuredMetrics.isEnabled() || configuredMetrics.isPushEnabled())
        && configuredMetrics.getMetricCategories().contains(metricCategory);
  }

  /**
   * Return true if a category with that name is already registered
   *
   * @param name the category name
   * @return true if a category with that name is already registered
   */
  public boolean containsMetricCategory(final String name) {
    return metricCategories.containsKey(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Return a metric category by name
   *
   * @param name the category name
   * @return the metric category or null if not registered
   */
  public @Nullable MetricCategory getMetricCategory(final String name) {
    return metricCategories.get(name.toUpperCase(Locale.ROOT));
  }

  /**
   * Set the metric configuration via a method since it is still not available when creating this
   * object
   *
   * @param metricsConfiguration the metrics configuration
   */
  public void setMetricsConfiguration(final MetricsConfiguration metricsConfiguration) {
    this.metricsConfiguration = metricsConfiguration;
  }
}
