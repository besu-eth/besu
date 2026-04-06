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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.BesuMetricCategory.DEFAULT_METRIC_CATEGORIES;
import static org.hyperledger.besu.metrics.BesuMetricCategory.NETWORK;
import static org.hyperledger.besu.metrics.BesuMetricCategory.PEERS;
import static org.hyperledger.besu.metrics.BesuMetricCategory.RPC;
import static org.hyperledger.besu.metrics.MetricsProtocol.OPENTELEMETRY;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.MetricsSystemFactory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.Observation;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.GlobalOpenTelemetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class OpenTelemetryMetricsSystemTest {

  private static final Comparator<Observation> IGNORE_VALUES =
      Comparator.<Observation, String>comparing(observation -> observation.category().getName())
          .thenComparing(Observation::metricName)
          .thenComparing((o1, o2) -> o1.labels().equals(o2.labels()) ? 0 : 1);

  @BeforeEach
  public void resetGlobalOpenTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  private OpenTelemetrySystem metricsSystem = null;

  private List<Observation> getObservation(final ObservableMetricsSystem metricsSystem)
      throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      Stream<Observation> observations = metricsSystem.streamObservations();
      List<Observation> result = observations.collect(Collectors.toList());
      if (!result.isEmpty()) {
        return result;
      }
      Thread.sleep(100);
    }
    return null;
  }

  @BeforeEach
  public void setUp() {
    metricsSystem = new OpenTelemetrySystem(DEFAULT_METRIC_CATEGORIES, true, "job", false);
  }

  @AfterEach
  public void tearDown() {
    metricsSystem.shutdown();
  }

  @Test
  public void shouldCreateObservationFromCounter() throws InterruptedException {
    final Counter counter = metricsSystem.createCounter(PEERS, "connected", "Some help string");

    counter.inc();
    assertThat(metricsSystem.streamObservations())
        .containsExactly(new Observation(PEERS, "connected", 1L, emptyList()));

    counter.inc();
    assertThat(getObservation(metricsSystem))
        .containsExactly(new Observation(PEERS, "connected", 2L, emptyList()));
  }

  @Test
  public void shouldHandleDuplicateCounterCreation() throws InterruptedException {
    final LabelledMetric<Counter> counter1 =
        metricsSystem.createLabelledCounter(PEERS, "connected", "Some help string");
    final LabelledMetric<Counter> counter2 =
        metricsSystem.createLabelledCounter(PEERS, "connected", "Some help string");
    assertThat(counter1).isEqualTo(counter2);

    counter1.labels().inc();
    assertThat(metricsSystem.streamObservations())
        .containsExactly(new Observation(PEERS, "connected", 1L, emptyList()));

    counter2.labels().inc();
    assertThat(getObservation(metricsSystem))
        .containsExactly(new Observation(PEERS, "connected", 2L, emptyList()));
  }

  @Test
  public void shouldCreateSeparateObservationsForEachCounterLabelValue() {
    final LabelledMetric<Counter> counter =
        metricsSystem.createLabelledCounter(PEERS, "connected", "Some help string", "labelName");

    counter.labels("value1").inc();
    counter.labels("value2").inc();
    counter.labels("value1").inc();

    assertThat(metricsSystem.streamObservations())
        .containsExactlyInAnyOrder(
            new Observation(PEERS, "connected", 2L, singletonList("value1")),
            new Observation(PEERS, "connected", 1L, singletonList("value2")));
  }

  @Test
  public void shouldIncrementCounterBySpecifiedAmount() throws InterruptedException {
    final Counter counter = metricsSystem.createCounter(PEERS, "connected", "Some help string");

    counter.inc(5);
    assertThat(metricsSystem.streamObservations())
        .containsExactly(new Observation(PEERS, "connected", 5L, emptyList()));

    counter.inc(6);
    assertThat(getObservation(metricsSystem))
        .containsExactly(new Observation(PEERS, "connected", 11L, emptyList()));
  }

  @Test
  public void shouldCreateObservationsFromTimer() {
    final OperationTimer timer = metricsSystem.createTimer(RPC, "request", "Some help");

    final OperationTimer.TimingContext context = timer.startTimer();
    context.stopTimer();

    assertThat(metricsSystem.streamObservations())
        .usingElementComparator(IGNORE_VALUES)
        .containsExactlyInAnyOrder(new Observation(RPC, "request", null, Collections.emptyList()));
  }

  @Test
  public void shouldHandleDuplicateTimerCreation() {
    final LabelledMetric<OperationTimer> timer1 =
        metricsSystem.createLabelledTimer(RPC, "request", "Some help");
    final LabelledMetric<OperationTimer> timer2 =
        metricsSystem.createLabelledTimer(RPC, "request", "Some help");
    assertThat(timer1).isEqualTo(timer2);
  }

  @Test
  public void shouldNotAccumulateCallbacksForTimer() {
    final LabelledMetric<OperationTimer> timer =
        metricsSystem.createLabelledTimer(RPC, "request", "Some help", "method");

    // Stop the timer many times — before the fix each stop() registered a new OTel callback.
    for (int i = 0; i < 10; i++) {
      try (final OperationTimer.TimingContext ignored = timer.labels("eth_getBlock").startTimer()) {
        // intentionally empty
      }
    }

    List<Observation> observations =
        metricsSystem
            .streamObservations()
            .filter(o -> o.metricName().equals("request"))
            .collect(Collectors.toList());

    // Despite 10 stops there must be exactly one observation per label set, not 10.
    assertThat(observations).hasSize(1);
    assertThat(observations.get(0).labels()).containsExactly("eth_getBlock");
    assertThat((Double) observations.get(0).value()).isPositive();
  }

  @Test
  public void shouldHandleDuplicateGaugeCreation() {
    metricsSystem.createGauge(RPC, "myGauge", "help", () -> 42.0);
    // Second call with the same name must not register a second OTel callback.
    metricsSystem.createGauge(RPC, "myGauge", "help", () -> 42.0);

    List<Observation> observations =
        metricsSystem.streamObservations().collect(Collectors.toList());
    assertThat(observations).hasSize(1);
    assertThat(observations).containsExactly(new Observation(RPC, "myGauge", 42.0, emptyList()));
  }

  @Test
  public void shouldHandleDuplicateSuppliedGaugeCreation() {
    final LabelledSuppliedMetric gauge1 =
        metricsSystem.createLabelledSuppliedGauge(RPC, "gaugeName", "help", "label");
    final LabelledSuppliedMetric gauge2 =
        metricsSystem.createLabelledSuppliedGauge(RPC, "gaugeName", "help", "label");

    // Both calls must return the exact same cached instance.
    assertThat(gauge1).isSameAs(gauge2);

    gauge1.labels(() -> 7.0, "value1");

    // Exactly one observation, not two.
    assertThat(metricsSystem.streamObservations())
        .containsExactly(new Observation(RPC, "gaugeName", 7.0, singletonList("value1")));
  }

  @Test
  public void shouldHandleDuplicateSuppliedCounterCreation() {
    final LabelledSuppliedMetric counter1 =
        metricsSystem.createLabelledSuppliedCounter(RPC, "myCounter", "help");
    final LabelledSuppliedMetric counter2 =
        metricsSystem.createLabelledSuppliedCounter(RPC, "myCounter", "help");

    // Both calls must return the exact same cached instance — only one OTel callback registered.
    assertThat(counter1).isSameAs(counter2);
  }

  @Test
  public void shouldCreateObservationsFromTimerWithLabels() {
    final LabelledMetric<OperationTimer> timer =
        metricsSystem.createLabelledTimer(RPC, "request", "Some help", "methodName");

    //noinspection EmptyTryBlock
    try (final OperationTimer.TimingContext ignored = timer.labels("method").startTimer()) {}
    //noinspection EmptyTryBlock
    try (final OperationTimer.TimingContext ignored = timer.labels("method").startTimer()) {}
    //noinspection EmptyTryBlock
    try (final OperationTimer.TimingContext ignored = timer.labels("method").startTimer()) {}
    //noinspection EmptyTryBlock
    try (final OperationTimer.TimingContext ignored = timer.labels("method").startTimer()) {}

    assertThat(metricsSystem.streamObservations())
        .usingElementComparator(IGNORE_VALUES) // We don't know how long it will actually take.
        .containsExactlyInAnyOrder(new Observation(RPC, "request", null, singletonList("method")));
  }

  @Test
  public void shouldNotCreateObservationsFromTimerWhenTimersDisabled() {
    OpenTelemetrySystem metricsSystem = null;
    try {
      metricsSystem = new OpenTelemetrySystem(DEFAULT_METRIC_CATEGORIES, false, "job", false);
      final LabelledMetric<OperationTimer> timer =
          metricsSystem.createLabelledTimer(RPC, "request", "Some help", "methodName");

      //noinspection EmptyTryBlock
      try (final OperationTimer.TimingContext ignored = timer.labels("method").startTimer()) {}

      assertThat(metricsSystem.streamObservations()).isEmpty();
    } finally {
      if (metricsSystem != null) {
        metricsSystem.shutdown();
      }
    }
  }

  @Test
  public void shouldCreateObservationFromGauge() {
    final MetricsConfiguration metricsConfiguration =
        MetricsConfiguration.builder()
            .metricCategories(ImmutableSet.of(BesuMetricCategory.RPC))
            .enabled(true)
            .protocol(OPENTELEMETRY)
            .build();
    OpenTelemetrySystem localMetricSystem = null;
    try {
      localMetricSystem =
          new OpenTelemetrySystem(
              metricsConfiguration.getMetricCategories(),
              metricsConfiguration.isTimersEnabled(),
              metricsConfiguration.getPrometheusJob(),
              false);
      localMetricSystem.initDefaults();
      localMetricSystem.createGauge(RPC, "myValue", "Help", () -> 7.0);

      assertThat(localMetricSystem.streamObservations())
          .containsExactlyInAnyOrder(new Observation(RPC, "myValue", 7.0, emptyList()));
    } finally {
      if (localMetricSystem != null) {
        localMetricSystem.shutdown();
      }
    }
  }

  @Test
  public void shouldCreateLabelledGauge() {
    LabelledSuppliedMetric labelledGauge =
        metricsSystem.createLabelledSuppliedGauge(RPC, "gaugeName", "help", "a", "b");
    labelledGauge.labels(() -> 1.0, "a1", "b1");
    labelledGauge.labels(() -> 11.0, "a2", "b2");
    labelledGauge.labels(() -> 21.0, "a3", "b3");

    assertThat(metricsSystem.streamObservations())
        .containsExactlyInAnyOrder(
            new Observation(RPC, "gaugeName", 1.0, List.of("a1", "b1")),
            new Observation(RPC, "gaugeName", 11.0, List.of("a2", "b2")),
            new Observation(RPC, "gaugeName", 21.0, List.of("a3", "b3")));
  }

  @Test
  public void shouldOnlyObserveEnabledMetrics() throws InterruptedException {
    final MetricsConfiguration metricsConfiguration =
        MetricsConfiguration.builder()
            .metricCategories(ImmutableSet.of(BesuMetricCategory.RPC))
            .enabled(true)
            .protocol(OPENTELEMETRY)
            .build();
    OpenTelemetrySystem localMetricSystem = null;
    try {
      localMetricSystem =
          new OpenTelemetrySystem(
              metricsConfiguration.getMetricCategories(),
              metricsConfiguration.isTimersEnabled(),
              metricsConfiguration.getPrometheusJob(),
              false);
      localMetricSystem.initDefaults();

      // do a category we are not watching
      final LabelledMetric<Counter> counterN =
          localMetricSystem.createLabelledCounter(
              NETWORK, "ABC", "Not that kind of network", "show");
      assertThat(counterN).isInstanceOf(NoOpMetricsSystem.LabelCountingNoOpMetric.class);

      counterN.labels("show").inc();
      assertThat(localMetricSystem.streamObservations()).isEmpty();

      // do a category we are watching
      final LabelledMetric<Counter> counterR =
          localMetricSystem.createLabelledCounter(RPC, "name", "Not useful", "method");
      assertThat(counterR).isNotInstanceOf(NoOpMetricsSystem.LabelCountingNoOpMetric.class);

      counterR.labels("op").inc();
      assertThat(getObservation(localMetricSystem))
          .containsExactly(new Observation(RPC, "name", (long) 1, singletonList("op")));
    } finally {
      if (localMetricSystem != null) {
        localMetricSystem.shutdown();
      }
    }
  }

  @Test
  public void returnsNoOpMetricsWhenAllDisabled() {
    final MetricsConfiguration metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(false)
            .pushEnabled(false)
            .protocol(OPENTELEMETRY)
            .build();
    final MetricsSystem localMetricSystem = MetricsSystemFactory.create(metricsConfiguration);

    assertThat(localMetricSystem).isInstanceOf(NoOpMetricsSystem.class);
  }

  @Test
  public void returnsPrometheusMetricsWhenEnabled() {
    final MetricsConfiguration metricsConfiguration =
        MetricsConfiguration.builder()
            .enabled(true)
            .pushEnabled(false)
            .protocol(OPENTELEMETRY)
            .build();
    OpenTelemetrySystem localMetricSystem = null;
    try {
      localMetricSystem = (OpenTelemetrySystem) MetricsSystemFactory.create(metricsConfiguration);

      assertThat(localMetricSystem).isInstanceOf(OpenTelemetrySystem.class);
    } finally {
      if (localMetricSystem != null) {
        localMetricSystem.shutdown();
      }
    }
  }
}
