/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.ExternalSummary;
import org.hyperledger.besu.plugin.services.metrics.Histogram;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedSummary;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Set;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.cache.Cache;

/** An interface for creating various Metrics components. */
public interface MetricsSystem extends BesuService {

  /**
   * Creates a Counter.
   *
   * @param category The {@link MetricCategory} this counter is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @return The created Counter instance.
   */
  default Counter createCounter(
      final MetricCategory category, final String name, final String help) {
    return createLabelledCounter(category, name, help, new String[0]).labels();
  }

  /**
   * Creates a Counter that gets its value from the specified supplier. To be used when the value of
   * the counter is calculated outside the metric system.
   *
   * @param category The {@link MetricCategory} this counter is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param valueSupplier The supplier of the value.
   */
  default void createCounter(
      final MetricCategory category,
      final String name,
      final String help,
      final DoubleSupplier valueSupplier) {
    createLabelledSuppliedCounter(category, name, help).labels(valueSupplier);
  }

  /**
   * Creates a Counter with assigned labels.
   *
   * @param category The {@link MetricCategory} this counter is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the Counter.
   * @return The created LabelledMetric instance.
   */
  LabelledMetric<Counter> createLabelledCounter(
      MetricCategory category, String name, String help, String... labelNames);

  /**
   * Creates a Counter with assigned labels, that gets its values from suppliers. To be used when
   * the values of the counter are calculated outside the metric system.
   *
   * @param category The {@link MetricCategory} this counter is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the Counter.
   * @return The created LabelledSupplierMetric instance.
   */
  LabelledSuppliedMetric createLabelledSuppliedCounter(
      MetricCategory category, String name, String help, String... labelNames);

  /**
   * Creates a Gauge with assigned labels, that gets its values from suppliers. To be used when the
   * values of the gauge are calculated outside the metric system.
   *
   * @param category The {@link MetricCategory} this gauge is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the Gauge.
   * @return The created LabelledGauge instance.
   */
  LabelledSuppliedMetric createLabelledSuppliedGauge(
      MetricCategory category, String name, String help, String... labelNames);

  /**
   * Creates a Timer.
   *
   * @param category The {@link MetricCategory} this timer is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @return The created Timer instance.
   */
  default OperationTimer createTimer(
      final MetricCategory category, final String name, final String help) {
    return createLabelledTimer(category, name, help, new String[0]).labels();
  }

  /**
   * Creates a Timer with assigned labels.
   *
   * @param category The {@link MetricCategory} this timer is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the Timer.
   * @return The created LabelledMetric instance.
   */
  LabelledMetric<OperationTimer> createLabelledTimer(
      MetricCategory category, String name, String help, String... labelNames);

  /**
   * Creates a simple Timer.
   *
   * @param category The {@link MetricCategory} this timer is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @return The created Timer instance.
   */
  default OperationTimer createSimpleTimer(
      final MetricCategory category, final String name, final String help) {
    return createSimpleLabelledTimer(category, name, help).labels();
  }

  /**
   * Creates a simple Timer with assigned labels.
   *
   * @param category The {@link MetricCategory} this timer is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the Timer.
   * @return The created Timer instance.
   */
  LabelledMetric<OperationTimer> createSimpleLabelledTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames);

  /**
   * Creates a gauge for displaying double vales. A gauge is a metric to report the current value.
   * The metric value may go up or down.
   *
   * @param category The {@link MetricCategory} this gauge is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param valueSupplier A supplier for the double value to be presented.
   */
  default void createGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final DoubleSupplier valueSupplier) {
    createLabelledSuppliedGauge(category, name, help).labels(valueSupplier);
  }

  /**
   * Creates a gauge for displaying integer values.
   *
   * @param category The {@link MetricCategory} this gauge is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param valueSupplier A supplier for the integer value to be presented.
   */
  default void createIntegerGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final IntSupplier valueSupplier) {
    createGauge(category, name, help, () -> (double) valueSupplier.getAsInt());
  }

  /**
   * Creates a gauge for displaying long values.
   *
   * @param category The {@link MetricCategory} this gauge is assigned to.
   * @param name A name for this metric.
   * @param help A human readable description of the metric.
   * @param valueSupplier A supplier for the long value to be presented.
   */
  default void createLongGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final LongSupplier valueSupplier) {
    createGauge(category, name, help, () -> (double) valueSupplier.getAsLong());
  }

  /**
   * Creates a histogram with assigned labels
   *
   * @param category The {@link MetricCategory} this histogram is assigned to.
   * @param name A name for this metric.
   * @param help A human-readable description of the metric.
   * @param buckets An array of buckets to assign to the histogram
   * @param labelNames An array of labels to assign to the histogram.
   * @return The labelled histogram.
   */
  LabelledMetric<Histogram> createLabelledHistogram(
      MetricCategory category, String name, String help, double[] buckets, String... labelNames);

  /**
   * Creates a histogram
   *
   * @param category The {@link MetricCategory} this histogram is assigned to.
   * @param name A name for this metric.
   * @param help A human-readable description of the metric.
   * @param buckets An array of buckets to assign to the histogram
   * @return The labelled histogram.
   */
  default Histogram createHistogram(
      final MetricCategory category, final String name, final String help, final double[] buckets) {
    return createLabelledHistogram(category, name, help, buckets).labels();
  }

  /**
   * Create a summary with assigned labels, that is computed externally to this metric system.
   * Useful when existing libraries calculate the summary data on their own, and we want to export
   * that summary via the configured metric system. A notable example are RocksDB statistics.
   *
   * @param category The {@link MetricCategory} this external summary is assigned to.
   * @param name A name for the metric.
   * @param help A human readable description of the metric.
   * @param labelNames An array of labels to assign to the supplier summary.
   * @return The created labelled supplied summary
   */
  LabelledSuppliedSummary createLabelledSuppliedSummary(
      MetricCategory category, String name, String help, String... labelNames);

  /**
   * Create a summary that is computed externally to this metric system. Useful when existing
   * libraries calculate the summary data on their own, and we want to export that summary via the
   * configured metric system. A notable example are RocksDB statistics.
   *
   * @param category The {@link MetricCategory} this external summary is assigned to.
   * @param name A name for the metric.
   * @param help A human readable description of the metric.
   * @param summarySupplier A supplier to retrieve the summary data when needed.
   */
  default void createSummary(
      final MetricCategory category,
      final String name,
      final String help,
      final Supplier<ExternalSummary> summarySupplier) {
    createLabelledSuppliedSummary(category, name, help).labels(summarySupplier);
  }

  /**
   * Collect metrics from Guava cache.
   *
   * @param category The {@link MetricCategory} this Guava cache is assigned to.
   * @param name the name to identify this Guava cache, must be unique.
   * @param cache the Guava cache
   */
  void createGuavaCacheCollector(MetricCategory category, String name, Cache<?, ?> cache);

  /**
   * Provides an immutable view into the metric categories enabled for metric collection.
   *
   * @return the set of enabled metric categories.
   */
  Set<MetricCategory> getEnabledCategories();

  /**
   * Checks if a particular category of metrics is enabled.
   *
   * @param category the category to check
   * @return true if the category is enabled, false otherwise
   */
  default boolean isCategoryEnabled(final MetricCategory category) {
    return getEnabledCategories().stream()
        .map(MetricCategory::getName)
        .anyMatch(category.getName()::equals);
  }
}
