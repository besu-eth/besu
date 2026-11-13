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
import org.hyperledger.besu.plugin.RegistrationClosedException;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.MetricCategoryRegistry;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

/** The Metric category registry implementation. */
public class MetricCategoryRegistryImpl implements MetricCategoryRegistry {
  /** The registrant of a category Besu added itself, outside any plugin's register(). */
  private static final String BESU = "Besu";

  /** A registered category with the id of the plugin that registered it. */
  private record Registered(String registrant, MetricCategory category) {}

  private final Map<String, Registered> metricCategories = new HashMap<>();
  private @Nullable MetricsConfiguration metricsConfiguration;
  private final BooleanSupplier registrationOpen;
  private final Supplier<Optional<String>> currentRegistrant;

  /** Creates a registry that accepts plugin categories at any time. */
  public MetricCategoryRegistryImpl() {
    this(() -> true);
  }

  /**
   * Creates a registry whose plugin categories all count as Besu's own.
   *
   * @param registrationOpen whether plugin registration is currently open; plugin categories added
   *     while it is closed are rejected. Besu's own categories, added through {@link
   *     #addCategories(Class)}, are not subject to it.
   */
  public MetricCategoryRegistryImpl(final BooleanSupplier registrationOpen) {
    this(registrationOpen, Optional::empty);
  }

  /**
   * Creates the registry.
   *
   * @param registrationOpen whether plugin registration is currently open; plugin categories added
   *     while it is closed are rejected. Besu's own categories, added through {@link
   *     #addCategories(Class)}, are not subject to it.
   * @param currentRegistrant the id of the plugin whose register() is running, empty when Besu
   *     itself is registering; a category name already registered by another registrant, Besu's own
   *     categories included, is rejected
   */
  public MetricCategoryRegistryImpl(
      final BooleanSupplier registrationOpen, final Supplier<Optional<String>> currentRegistrant) {
    this.registrationOpen = registrationOpen;
    this.currentRegistrant = currentRegistrant;
  }

  /**
   * Add Metrics categories.
   *
   * @param <T> the type parameter
   * @param categoryEnum the category enum
   */
  public <T extends Enum<T> & MetricCategory> void addCategories(final Class<T> categoryEnum) {
    EnumSet.allOf(categoryEnum)
        .forEach(category -> metricCategories.put(category.name(), new Registered(BESU, category)));
  }

  /**
   * Add registry category.
   *
   * @param metricCategory the metric category
   */
  @Override
  public void addMetricCategory(final MetricCategory metricCategory) {
    if (!registrationOpen.getAsBoolean()) {
      throw new RegistrationClosedException("MetricCategoryRegistry", "addMetricCategory");
    }
    final String name = metricCategory.getName().toUpperCase(Locale.ROOT);
    final String registrant = currentRegistrant.get().orElse(BESU);
    final Registered previous = metricCategories.get(name);
    if (previous != null && !previous.registrant().equals(registrant)) {
      throw new IllegalStateException(
          "cannot register metric category "
              + metricCategory.getName()
              + ": "
              + owner(previous.registrant())
              + " already registered it");
    }
    metricCategories.put(name, new Registered(registrant, metricCategory));
  }

  private static String owner(final String registrant) {
    return BESU.equals(registrant) ? BESU : "plugin " + registrant;
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
    final Registered registered = metricCategories.get(name.toUpperCase(Locale.ROOT));
    return registered == null ? null : registered.category();
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
