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
package org.hyperledger.besu.metrics.prometheus;

import static java.util.Objects.requireNonNull;
import static org.hyperledger.besu.metrics.prometheus.PrometheusCollector.getLabelValues;

import org.hyperledger.besu.metrics.Observation;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import io.prometheus.metrics.instrumentation.guava.CacheMetricsCollector;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.vertx.core.impl.ConcurrentHashSet;

/**
 * A Prometheus Guava cache collector implementation for Besu metrics. This class provides a way to
 * expose metrics from Guava caches, it behaves differently from other collectors, since instead of
 * having one collector per cache, Prometheus provides only one collector for all caches, so we need
 * a Context that wraps the Prometheus single collector and handles its registration, while here we
 * keep the abstraction of one Prometheus collector for one Guava cache, and we also verify that
 * there is no collector name clash.
 */
class PrometheusGuavaCache extends CategorizedPrometheusCollector {
  /** Use to reduce the possibility of a name clash with other collectors */
  private static final String NAME_PREFIX = "__guavaCacheMetricsCollector__";

  private final Cache<?, ?> cache;
  private final Context context;

  public PrometheusGuavaCache(
      final MetricCategory category,
      final Context context,
      final String name,
      final Cache<?, ?> cache) {
    super(category, name);
    if (context.alreadyExists(name)) {
      throw new IllegalStateException("Cache already registered: " + name);
    }
    this.cache = cache;
    this.context = context;
  }

  @Override
  public String getIdentifier() {
    return category.getName() + "." + NAME_PREFIX + "." + name;
  }

  @Override
  public void register(final PrometheusRegistry registry) {
    context.registerCache(registry, name, cache);
  }

  @Override
  public void unregister(final PrometheusRegistry registry) {
    context.unregisterCache(registry, name);
  }

  @Override
  public Stream<Observation> streamObservations() {
    return context.streamObservations(category, name);
  }

  /**
   * Since Prometheus provides only one collector for all Guava caches, we only need to register
   * that collector once when the first Besu Guava cache collector is created, and unregister it
   * when the last is unregistered, so we have this context to keep track of that and also manage
   * the observations stream.
   */
  static class Context {
    private static final Map<String, ToDoubleFunction<DataPointSnapshot>>
        COLLECTOR_VALUE_EXTRACTORS =
            Map.of(
                "guava_cache_eviction", Context::counterValueExtractor,
                "guava_cache_hit", Context::counterValueExtractor,
                "guava_cache_miss", Context::counterValueExtractor,
                "guava_cache_requests", Context::counterValueExtractor,
                "guava_cache_size", Context::gaugeValueExtractor);

    private final CacheMetricsCollector cacheMetricsCollector = new CacheMetricsCollector();
    private final Set<String> cacheNames = new ConcurrentHashSet<>();
    private final AtomicBoolean collectorRegistered = new AtomicBoolean(false);

    boolean alreadyExists(final String name) {
      return cacheNames.contains(name);
    }

    void registerCache(
        final PrometheusRegistry registry, final String name, final Cache<?, ?> cache) {
      cacheMetricsCollector.addCache(name, cache);
      cacheNames.add(name);
      if (collectorRegistered.compareAndSet(false, true)) {
        registry.register(cacheMetricsCollector);
      }
    }

    void unregisterCache(final PrometheusRegistry registry, final String name) {
      cacheMetricsCollector.removeCache(name);
      cacheNames.remove(name);
      if (cacheNames.isEmpty() && collectorRegistered.compareAndSet(true, false)) {
        registry.unregister(cacheMetricsCollector);
      }
    }

    void clear() {
      cacheNames.forEach(cacheMetricsCollector::removeCache);
      cacheNames.clear();
      collectorRegistered.set(false);
    }

    private Stream<Observation> streamObservations(
        final MetricCategory category, final String cacheName) {
      return cacheMetricsCollector.collect().stream()
          .flatMap(ms -> convertToObservations(category, cacheName, ms));
    }

    private static Stream<Observation> convertToObservations(
        final MetricCategory category, final String cacheName, final MetricSnapshot snapshot) {
      final var prometheusName = snapshot.getMetadata().getPrometheusName();
      if (COLLECTOR_VALUE_EXTRACTORS.containsKey(prometheusName)) {
        final var valueExtractor = requireNonNull(COLLECTOR_VALUE_EXTRACTORS.get(prometheusName));
        return snapshotToObservations(
            category, cacheName, prometheusName, valueExtractor, snapshot);
      }
      return Stream.empty();
    }

    private static Stream<Observation> snapshotToObservations(
        final MetricCategory category,
        final String cacheName,
        final String prometheusName,
        final ToDoubleFunction<DataPointSnapshot> valueExtractor,
        final MetricSnapshot snapshot) {
      return snapshot.getDataPoints().stream()
          .filter(gdps -> gdps.getLabels().get("cache").equals(cacheName))
          .map(
              gdps ->
                  new Observation(
                      category,
                      prometheusName,
                      valueExtractor.applyAsDouble(gdps),
                      getLabelValues(gdps.getLabels())));
    }

    private static double gaugeValueExtractor(final DataPointSnapshot snapshot) {
      return ((GaugeSnapshot.GaugeDataPointSnapshot) snapshot).getValue();
    }

    private static double counterValueExtractor(final DataPointSnapshot snapshot) {
      return ((CounterSnapshot.CounterDataPointSnapshot) snapshot).getValue();
    }
  }
}
