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
package org.hyperledger.besu.metrics.prometheus;

import static java.util.Map.entry;

import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.Observation;
import org.hyperledger.besu.metrics.StandardMetricCategory;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.Histogram;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedSummary;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import io.prometheus.metrics.instrumentation.jvm.JvmBufferPoolMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmClassLoadingMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmCompilationMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmGarbageCollectorMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmMemoryMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmMemoryPoolAllocationMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmNativeMemoryMetrics;
import io.prometheus.metrics.instrumentation.jvm.JvmRuntimeInfoMetric;
import io.prometheus.metrics.instrumentation.jvm.JvmThreadsMetrics;
import io.prometheus.metrics.instrumentation.jvm.ProcessMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

/** The Prometheus metrics system. */
public class PrometheusMetricsSystem implements ObservableMetricsSystem {
  private static final Map<Double, Double> DEFAULT_SUMMARY_QUANTILES =
      Map.ofEntries(
          entry(0.2, 0.02),
          entry(0.5, 0.05),
          entry(0.8, 0.02),
          entry(0.95, 0.005),
          entry(0.99, 0.001),
          entry(1.0, 0.0));

  private final Map<MetricCategory, Collection<PrometheusCollector>> collectors =
      new ConcurrentHashMap<>();
  private final PrometheusRegistry registry = PrometheusRegistry.defaultRegistry;
  private final Map<CachedMetricKey, LabelledMetric<Counter>> cachedCounters =
      new ConcurrentHashMap<>();
  private final Map<CachedMetricKey, LabelledMetric<OperationTimer>> cachedTimers =
      new ConcurrentHashMap<>();
  private final Map<CachedMetricKey, LabelledMetric<Histogram>> cachedHistograms =
      new ConcurrentHashMap<>();

  private final PrometheusGuavaCache.Context guavaCacheCollectorContext =
      new PrometheusGuavaCache.Context();

  private final Set<MetricCategory> enabledCategories;
  private final boolean timersEnabled;

  /**
   * Instantiates a new Prometheus metrics system.
   *
   * @param enabledCategories the enabled categories
   * @param timersEnabled the timers enabled
   */
  public PrometheusMetricsSystem(
      final Set<MetricCategory> enabledCategories, final boolean timersEnabled) {
    this.enabledCategories = ImmutableSet.copyOf(enabledCategories);
    this.timersEnabled = timersEnabled;
  }

  /** Init. */
  public void init() {
    if (isCategoryEnabled(StandardMetricCategory.JVM)) {
      JvmThreadsMetrics.builder().register(registry);
      JvmBufferPoolMetrics.builder().register(registry);
      JvmClassLoadingMetrics.builder().register(registry);
      JvmCompilationMetrics.builder().register(registry);
      JvmGarbageCollectorMetrics.builder().register(registry);
      JvmMemoryMetrics.builder().register(registry);
      JvmMemoryPoolAllocationMetrics.builder().register(registry);
      JvmNativeMemoryMetrics.builder().register(registry);
      JvmRuntimeInfoMetric.builder().register(registry);
    }
    if (isCategoryEnabled(StandardMetricCategory.PROCESS)) {
      ProcessMetrics.builder().register(registry);
    }
  }

  @Override
  public Set<MetricCategory> getEnabledCategories() {
    return enabledCategories;
  }

  @Override
  public LabelledMetric<Counter> createLabelledCounter(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    return cachedCounters.computeIfAbsent(
        CachedMetricKey.of(category, name),
        k -> {
          if (isCategoryEnabled(category)) {
            final var counter = new PrometheusCounter(category, name, help, labelNames);
            registerCollector(category, counter);
            return counter;
          }
          return NoOpMetricsSystem.getCounterLabelledMetric(labelNames.length);
        });
  }

  @Override
  public LabelledMetric<OperationTimer> createLabelledTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    return cachedTimers.computeIfAbsent(
        CachedMetricKey.of(category, name),
        k -> {
          if (timersEnabled && isCategoryEnabled(category)) {
            final var summary =
                new PrometheusTimer(category, name, help, DEFAULT_SUMMARY_QUANTILES, labelNames);
            registerCollector(category, summary);
            return summary;
          }
          return NoOpMetricsSystem.getOperationTimerLabelledMetric(labelNames.length);
        });
  }

  @Override
  public LabelledMetric<OperationTimer> createSimpleLabelledTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    return cachedTimers.computeIfAbsent(
        CachedMetricKey.of(category, name),
        k -> {
          if (timersEnabled && isCategoryEnabled(category)) {
            final var histogram =
                new PrometheusSimpleTimer(category, name, help, new double[] {1D}, labelNames);
            registerCollector(category, histogram);
            return histogram;
          }
          return NoOpMetricsSystem.getOperationTimerLabelledMetric(labelNames.length);
        });
  }

  @Override
  public LabelledMetric<Histogram> createLabelledHistogram(
      final MetricCategory category,
      final String name,
      final String help,
      final double[] buckets,
      final String... labelNames) {
    return cachedHistograms.computeIfAbsent(
        CachedMetricKey.of(category, name),
        k -> {
          if (isCategoryEnabled(category)) {
            final var histogram =
                new PrometheusHistogram(category, name, help, buckets, labelNames);
            registerCollector(category, histogram);
            return histogram;
          }
          return NoOpMetricsSystem.getHistogramLabelledMetric(labelNames.length);
        });
  }

  @Override
  public LabelledSuppliedSummary createLabelledSuppliedSummary(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    if (isCategoryEnabled(category)) {
      final PrometheusSuppliedSummary summary =
          new PrometheusSuppliedSummary(category, name, help, labelNames);
      registerCollector(category, summary);
      return summary;
    }
    return NoOpMetricsSystem.getLabelledSuppliedSummary(labelNames.length);
  }

  @Override
  public void createGuavaCacheCollector(
      final MetricCategory category, final String name, final Cache<?, ?> cache) {
    if (isCategoryEnabled(category)) {
      final var cacheCollector =
          new PrometheusGuavaCache(category, guavaCacheCollectorContext, name, cache);
      registerCollector(category, cacheCollector);
    }
  }

  @Override
  public LabelledSuppliedMetric createLabelledSuppliedCounter(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    if (isCategoryEnabled(category)) {
      final PrometheusSuppliedCounter counter =
          new PrometheusSuppliedCounter(category, name, help, labelNames);
      registerCollector(category, counter);
      return counter;
    }
    return NoOpMetricsSystem.getLabelledSuppliedMetric(labelNames.length);
  }

  @Override
  public LabelledSuppliedMetric createLabelledSuppliedGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    if (isCategoryEnabled(category)) {
      final PrometheusSuppliedGauge gauge =
          new PrometheusSuppliedGauge(category, name, help, labelNames);
      registerCollector(category, gauge);
      return gauge;
    }
    return NoOpMetricsSystem.getLabelledSuppliedMetric(labelNames.length);
  }

  private void registerCollector(
      final MetricCategory category, final PrometheusCollector collector) {
    final Collection<PrometheusCollector> categoryCollectors =
        this.collectors.computeIfAbsent(category, key -> ConcurrentHashMap.newKeySet());

    // unregister if already present
    categoryCollectors.stream()
        .filter(c -> c.getIdentifier().equals(collector.getIdentifier()))
        .findFirst()
        .ifPresent(
            c -> {
              categoryCollectors.remove(c);
              c.unregister(registry);
            });

    collector.register(registry);
    categoryCollectors.add(collector);
  }

  @Override
  public Stream<Observation> streamObservations(final MetricCategory category) {
    return collectors.getOrDefault(category, Collections.emptySet()).stream()
        .flatMap(PrometheusCollector::streamObservations);
  }

  @Override
  public Stream<Observation> streamObservations() {
    return collectors.keySet().stream().flatMap(this::streamObservations);
  }

  PrometheusRegistry getRegistry() {
    return registry;
  }

  @Override
  public void shutdown() {
    registry.clear();
    collectors.clear();
    cachedCounters.clear();
    cachedTimers.clear();
    guavaCacheCollectorContext.clear();
  }

  private record CachedMetricKey(MetricCategory category, String name) {
    static CachedMetricKey of(final MetricCategory category, final String name) {
      return new CachedMetricKey(category, name);
    }
  }
}
