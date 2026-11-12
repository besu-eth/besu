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
package org.hyperledger.besu.metrics.opentelemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.BesuMetricCategory.NETWORK;
import static org.hyperledger.besu.metrics.BesuMetricCategory.RPC;

import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.Observation;
import org.hyperledger.besu.metrics.prometheus.PrometheusMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class OpenTelemetryMetricIdentityTest {
  private static final String NAME = "shared_metric";
  private ObservableMetricsSystem metrics;

  @AfterEach
  void shutdown() {
    if (metrics != null) {
      metrics.shutdown();
    }
  }

  enum Kind {
    COUNTER,
    TIMER
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void keepsTheSameNameInDifferentEnabledCategories(final Kind kind) {
    metrics = new OpenTelemetrySystem(Set.of(RPC, NETWORK), true, "test", false);
    record(metrics, RPC, kind);
    record(metrics, NETWORK, kind);
    assertThat(metrics.streamObservations().map(Observation::category))
        .containsExactlyInAnyOrder(RPC, NETWORK);
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void disabledCategoryRegisteredFirstDoesNotDisableEnabledCategory(final Kind kind) {
    metrics = new OpenTelemetrySystem(Set.of(RPC), true, "test", false);
    record(metrics, NETWORK, kind);
    record(metrics, RPC, kind);
    assertThat(metrics.streamObservations().map(Observation::category)).containsExactly(RPC);
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void disabledCategoryRegisteredLastDoesNotReportUnderEnabledCategory(final Kind kind) {
    metrics = new OpenTelemetrySystem(Set.of(RPC), true, "test", false);
    record(metrics, RPC, kind);
    record(metrics, NETWORK, kind);
    assertThat(metrics.streamObservations().map(Observation::labels))
        .containsExactly(List.of(RPC.getName()));
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void sameCategoryStillReusesItsMetric(final Kind kind) {
    metrics = new OpenTelemetrySystem(Set.of(RPC), true, "test", false);
    assertThat(createMetric(metrics, RPC, kind)).isSameAs(createMetric(metrics, RPC, kind));
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void prometheusKeepsCategoriesSeparate(final Kind kind) {
    metrics = new PrometheusMetricsSystem(Set.of(RPC, NETWORK), true);
    record(metrics, RPC, kind);
    record(metrics, NETWORK, kind);
    assertThat(metrics.streamObservations().map(Observation::category).distinct())
        .containsExactlyInAnyOrder(RPC, NETWORK);
  }

  private static Object createMetric(
      final ObservableMetricsSystem metrics, final MetricCategory category, final Kind kind) {
    return switch (kind) {
      case COUNTER -> metrics.createLabelledCounter(category, NAME, "test");
      case TIMER -> metrics.createLabelledTimer(category, NAME, "test");
    };
  }

  private static void record(
      final ObservableMetricsSystem metrics, final MetricCategory category, final Kind kind) {
    switch (kind) {
      case COUNTER ->
          metrics
              .createLabelledCounter(category, NAME, "test", "source")
              .labels(category.getName())
              .inc();
      case TIMER ->
          metrics
              .createLabelledTimer(category, NAME, "test", "source")
              .labels(category.getName())
              .startTimer()
              .stopTimer();
    }
  }
}
