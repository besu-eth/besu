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
package org.hyperledger.besu.metrics.opentelemetry;

import org.hyperledger.besu.metrics.BesuMetricCategory;
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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.stream.Stream;
import javax.inject.Singleton;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.incubating.ServiceIncubatingAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Metrics system relying on the native OpenTelemetry format. */
@Singleton
public class OpenTelemetrySystem implements ObservableMetricsSystem {

  private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetrySystem.class);

  private static final String TYPE_LABEL_KEY = "type";
  private static final String AREA_LABEL_KEY = "area";
  private static final String POOL_LABEL_KEY = "pool";
  private static final String USED = "used";
  private static final String COMMITTED = "committed";
  private static final String MAX = "max";
  private static final String HEAP = "heap";
  private static final String NON_HEAP = "non_heap";

  private final Set<MetricCategory> enabledCategories;
  private final boolean timersEnabled;
  private final Map<String, LabelledMetric<Counter>> cachedCounters = new ConcurrentHashMap<>();
  private final Map<String, LabelledMetric<OperationTimer>> cachedTimers =
      new ConcurrentHashMap<>();
  private final Map<String, LabelledSuppliedMetric> cachedSuppliedGauges =
      new ConcurrentHashMap<>();
  private final Map<String, LabelledSuppliedMetric> cachedSuppliedCounters =
      new ConcurrentHashMap<>();
  private final Set<String> registeredGaugeNames = ConcurrentHashMap.newKeySet();
  private final SdkMeterProvider sdkMeterProvider;
  private final DebugMetricReader debugMetricReader;
  private final SdkTracerProvider sdkTracerProvider;

  /**
   * Instantiates a new Open telemetry system.
   *
   * @param enabledCategories the enabled categories
   * @param timersEnabled the timers enabled
   * @param jobName the job name
   * @param setAsGlobal the set as global
   */
  public OpenTelemetrySystem(
      final Set<MetricCategory> enabledCategories,
      final boolean timersEnabled,
      final String jobName,
      final boolean setAsGlobal) {
    LOG.info("Starting OpenTelemetry metrics system");
    this.enabledCategories = ImmutableSet.copyOf(enabledCategories);
    this.timersEnabled = timersEnabled;
    this.debugMetricReader = new DebugMetricReader();
    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.builder()
                        .put(ServiceIncubatingAttributes.SERVICE_NAME, jobName)
                        .build()));
    AutoConfiguredOpenTelemetrySdkBuilder autoSdkBuilder =
        AutoConfiguredOpenTelemetrySdk.builder()
            .addMeterProviderCustomizer(
                (provider, config) ->
                    provider.setResource(resource).registerMetricReader(debugMetricReader))
            .addTracerProviderCustomizer((provider, config) -> provider.setResource(resource));

    if (setAsGlobal) {
      autoSdkBuilder.setResultAsGlobal();
    }

    OpenTelemetrySdk sdk = autoSdkBuilder.build().getOpenTelemetrySdk();
    this.sdkMeterProvider = sdk.getSdkMeterProvider();
    this.sdkTracerProvider = sdk.getSdkTracerProvider();
  }

  @Override
  public Stream<Observation> streamObservations(final MetricCategory category) {
    return streamObservations().filter(metricData -> metricData.category().equals(category));
  }

  @Override
  public Stream<Observation> streamObservations() {
    Collection<MetricData> metricsList = this.debugMetricReader.getAllMetrics();
    return metricsList.stream().flatMap(this::convertToObservations);
  }

  private Stream<Observation> convertToObservations(final MetricData metricData) {
    List<Observation> observations = new ArrayList<>();
    MetricCategory category =
        categoryNameToMetricCategory(metricData.getInstrumentationScopeInfo().getName());
    if (category == null) {
      return Stream.empty();
    }
    Collection<?> points;
    switch (metricData.getType()) {
      case DOUBLE_GAUGE:
        points = metricData.getDoubleGaugeData().getPoints();
        break;
      case DOUBLE_SUM:
        points = metricData.getDoubleSumData().getPoints();
        break;
      case SUMMARY:
        points = metricData.getData().getPoints();
        break;
      case LONG_SUM:
        points = metricData.getLongSumData().getPoints();
        break;
      case HISTOGRAM:
        points = metricData.getData().getPoints();
        break;
      case LONG_GAUGE:
        points = metricData.getLongGaugeData().getPoints();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type " + metricData.getType().name());
    }

    for (Object ptObj : points) {
      PointData point = (PointData) ptObj;
      List<String> labels = new ArrayList<>();
      point.getAttributes().forEach((k, v) -> labels.add(v.toString()));
      observations.add(
          new Observation(
              category, metricData.getName(), extractValue(metricData.getType(), point), labels));
    }
    return observations.stream();
  }

  private MetricCategory categoryNameToMetricCategory(final String name) {
    Set<MetricCategory> categories =
        ImmutableSet.<MetricCategory>builder()
            .addAll(EnumSet.allOf(BesuMetricCategory.class))
            .addAll(EnumSet.allOf(StandardMetricCategory.class))
            .build();
    for (MetricCategory category : categories) {
      if (category.getName().equals(name)) {
        return category;
      }
    }
    return null;
  }

  private Object extractValue(final MetricDataType type, final PointData point) {
    switch (type) {
      case LONG_GAUGE:
      case LONG_SUM:
        return ((LongPointData) point).getValue();
      case DOUBLE_GAUGE:
        return ((DoublePointData) point).getValue();
      case SUMMARY:
        return ((SummaryPointData) point).getValues();
      case HISTOGRAM:
        return ((HistogramPointData) point).getCounts();
      default:
        throw new UnsupportedOperationException("Unsupported type " + type);
    }
  }

  @Override
  public LabelledMetric<Counter> createLabelledCounter(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    LOG.trace("Creating a counter {}", name);
    return cachedCounters.computeIfAbsent(
        name,
        (k) -> {
          if (isCategoryEnabled(category)) {
            final Meter meter = sdkMeterProvider.get(category.getName());

            final LongCounter counter = meter.counterBuilder(name).setDescription(help).build();
            return new OpenTelemetryCounter(counter, labelNames);
          } else {
            return NoOpMetricsSystem.getCounterLabelledMetric(labelNames.length);
          }
        });
  }

  @Override
  public LabelledMetric<OperationTimer> createSimpleLabelledTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    return createLabelledTimer(category, name, help, labelNames);
  }

  @Override
  public LabelledSuppliedSummary createLabelledSuppliedSummary(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    // not yet supported
    return (LabelledSuppliedSummary) NoOpMetricsSystem.getLabelledSuppliedMetric(labelNames.length);
  }

  @Override
  public LabelledMetric<OperationTimer> createLabelledTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    LOG.trace("Creating a timer {}", name);
    return cachedTimers.computeIfAbsent(
        name,
        (k) -> {
          if (timersEnabled && isCategoryEnabled(category)) {
            final Meter meter = sdkMeterProvider.get(category.getName());
            return new OpenTelemetryTimer(name, help, meter, labelNames);
          } else {
            return NoOpMetricsSystem.getOperationTimerLabelledMetric(labelNames.length);
          }
        });
  }

  @Override
  public void createGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final DoubleSupplier valueSupplier) {
    LOG.trace("Creating a gauge {}", name);
    if (isCategoryEnabled(category) && registeredGaugeNames.add(name)) {
      final Meter meter = sdkMeterProvider.get(category.getName());
      meter
          .gaugeBuilder(name)
          .setDescription(help)
          .buildWithCallback(res -> res.record(valueSupplier.getAsDouble(), Attributes.empty()));
    }
  }

  @Override
  public LabelledMetric<Histogram> createLabelledHistogram(
      final MetricCategory category,
      final String name,
      final String help,
      final double[] buckets,
      final String... labelNames) {
    // not yet supported
    return NoOpMetricsSystem.getHistogramLabelledMetric(labelNames.length);
  }

  @Override
  public void createGuavaCacheCollector(
      final MetricCategory category, final String name, final Cache<?, ?> cache) {}

  @Override
  public LabelledSuppliedMetric createLabelledSuppliedCounter(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    LOG.trace("Creating a labelled supplied counter {}", name);
    if (isCategoryEnabled(category)) {
      return cachedSuppliedCounters.computeIfAbsent(
          name,
          k ->
              new OpenTelemetrySuppliedCounter(
                  name, help, sdkMeterProvider.get(category.getName()), List.of(labelNames)));
    }
    return NoOpMetricsSystem.getLabelledSuppliedMetric(labelNames.length);
  }

  @Override
  public LabelledSuppliedMetric createLabelledSuppliedGauge(
      final MetricCategory category,
      final String name,
      final String help,
      final String... labelNames) {
    LOG.trace("Creating a labelled gauge {}", name);
    if (isCategoryEnabled(category)) {
      return cachedSuppliedGauges.computeIfAbsent(
          name,
          k ->
              new OpenTelemetryGauge(
                  name, help, sdkMeterProvider.get(category.getName()), List.of(labelNames)));
    }
    return NoOpMetricsSystem.getLabelledSuppliedMetric(labelNames.length);
  }

  @Override
  public Set<MetricCategory> getEnabledCategories() {
    return enabledCategories;
  }

  /** Init defaults. */
  public void initDefaults() {
    if (isCategoryEnabled(StandardMetricCategory.JVM)) {
      collectGC();
    }
  }

  private void collectGC() {
    final List<GarbageCollectorMXBean> garbageCollectors =
        ManagementFactory.getGarbageCollectorMXBeans();
    final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    final List<MemoryPoolMXBean> poolBeans = ManagementFactory.getMemoryPoolMXBeans();
    final Meter meter = sdkMeterProvider.get(StandardMetricCategory.JVM.getName());
    final List<Attributes> labelSets = new ArrayList<>(garbageCollectors.size());
    for (final GarbageCollectorMXBean gc : garbageCollectors) {
      labelSets.add(Attributes.of(AttributeKey.stringKey("gc"), gc.getName()));
    }
    meter
        .gaugeBuilder("jvm.gc.collection")
        .setDescription("Time spent in a given JVM garbage collector in milliseconds.")
        .setUnit("ms")
        .buildWithCallback(
            resultLongObserver -> {
              for (int i = 0; i < garbageCollectors.size(); i++) {
                resultLongObserver.record(
                    (double) garbageCollectors.get(i).getCollectionTime(), labelSets.get(i));
              }
            });
    final AttributeKey<String> typeKey = AttributeKey.stringKey(TYPE_LABEL_KEY);
    final AttributeKey<String> areaKey = AttributeKey.stringKey(AREA_LABEL_KEY);
    final Attributes usedHeap = Attributes.of(typeKey, USED, areaKey, HEAP);
    final Attributes usedNonHeap = Attributes.of(typeKey, USED, areaKey, NON_HEAP);
    final Attributes committedHeap = Attributes.of(typeKey, COMMITTED, areaKey, HEAP);
    final Attributes committedNonHeap = Attributes.of(typeKey, COMMITTED, areaKey, NON_HEAP);
    // TODO: Decide if max is needed or not. May be derived with some approximation from max(used).
    final Attributes maxHeap = Attributes.of(typeKey, MAX, areaKey, HEAP);
    final Attributes maxNonHeap = Attributes.of(typeKey, MAX, areaKey, NON_HEAP);
    meter
        .upDownCounterBuilder("jvm.memory.area")
        .setDescription("Bytes of a given JVM memory area.")
        .setUnit("By")
        .buildWithCallback(
            resultLongObserver -> {
              MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
              MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
              resultLongObserver.record(heapUsage.getUsed(), usedHeap);
              resultLongObserver.record(nonHeapUsage.getUsed(), usedNonHeap);
              resultLongObserver.record(heapUsage.getUsed(), committedHeap);
              resultLongObserver.record(nonHeapUsage.getUsed(), committedNonHeap);
              resultLongObserver.record(heapUsage.getUsed(), maxHeap);
              resultLongObserver.record(nonHeapUsage.getUsed(), maxNonHeap);
            });
    final List<Attributes> usedLabelSets = new ArrayList<>(poolBeans.size());
    final List<Attributes> committedLabelSets = new ArrayList<>(poolBeans.size());
    final List<Attributes> maxLabelSets = new ArrayList<>(poolBeans.size());
    final AttributeKey<String> poolKey = AttributeKey.stringKey(POOL_LABEL_KEY);
    for (final MemoryPoolMXBean pool : poolBeans) {
      usedLabelSets.add(Attributes.of(typeKey, USED, poolKey, pool.getName()));
      committedLabelSets.add(Attributes.of(typeKey, COMMITTED, poolKey, pool.getName()));
      maxLabelSets.add(Attributes.of(typeKey, MAX, poolKey, pool.getName()));
    }

    meter
        .upDownCounterBuilder("jvm.memory.pool")
        .setDescription("Bytes of a given JVM memory pool.")
        .setUnit("By")
        .buildWithCallback(
            resultLongObserver -> {
              for (int i = 0; i < poolBeans.size(); i++) {
                MemoryUsage poolUsage = poolBeans.get(i).getUsage();
                resultLongObserver.record(poolUsage.getUsed(), usedLabelSets.get(i));
                resultLongObserver.record(poolUsage.getCommitted(), committedLabelSets.get(i));
                // TODO: Decide if max is needed or not. May be derived with some approximation from
                //  max(used).
                resultLongObserver.record(poolUsage.getMax(), maxLabelSets.get(i));
              }
            });
  }

  /** Shuts down the OpenTelemetry exporters, blocking until they have completed orderly. */
  @Override
  public void shutdown() {
    final CompletableResultCode result =
        CompletableResultCode.ofAll(
            Arrays.asList(this.sdkMeterProvider.shutdown(), this.sdkTracerProvider.shutdown()));
    result.join(5000, TimeUnit.SECONDS);
  }

  /**
   * Gets tracer provider.
   *
   * @return the tracer provider
   */
  public TracerProvider getTracerProvider() {
    return sdkTracerProvider;
  }
}
