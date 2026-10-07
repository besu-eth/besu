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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.metrics.SyncDurationMetrics.Labels;
import org.hyperledger.besu.metrics.prometheus.PrometheusMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SyncDurationMetricsTest {

  private PrometheusMetricsSystem metricsSystem;
  private SyncDurationMetrics syncDurationMetrics;

  @BeforeEach
  void setUp() {
    metricsSystem =
        new PrometheusMetricsSystem(ImmutableSet.of(BesuMetricCategory.SYNCHRONIZER), true);
    syncDurationMetrics = new SyncDurationMetrics(metricsSystem);
  }

  @AfterEach
  void tearDown() {
    metricsSystem.shutdown();
  }

  @Test
  void recordsTheDurationOfACompletedPhase() {
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).hasValue(1L);
  }

  @Test
  void recordsNothingWhileThePhaseIsStillRunning() {
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).isEmpty();
  }

  @Test
  void recordsNothingWhenStoppingAPhaseThatNeverStarted() {
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).isEmpty();
  }

  @Test
  void recordsOnlyTheFirstCompletedMeasurementOfAPhase() {
    // First (successful) chain download.
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    // A later re-pivot restarts the chain download with a new downloader: it must not be reported.
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).hasValue(1L);
  }

  @Test
  @SuppressWarnings("unchecked")
  void restartingARunningPhaseKeepsTheOriginalStartTime() {
    final MetricsSystem mockedMetricsSystem = mock(MetricsSystem.class);
    final LabelledMetric<OperationTimer> labelledTimer = mock(LabelledMetric.class);
    final OperationTimer operationTimer = mock(OperationTimer.class);
    final OperationTimer.TimingContext timingContext = mock(OperationTimer.TimingContext.class);
    when(mockedMetricsSystem.createSimpleLabelledTimer(
            any(MetricCategory.class), anyString(), anyString(), any(String[].class)))
        .thenReturn(labelledTimer);
    when(labelledTimer.labels(Labels.CHAIN_DOWNLOAD_DURATION.name())).thenReturn(operationTimer);
    when(operationTimer.startTimer()).thenReturn(timingContext);
    final SyncDurationMetrics metrics = new SyncDurationMetrics(mockedMetricsSystem);

    metrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    // A re-pivot creates a new chain downloader which starts the timer again while the previous
    // measurement is still running: the original timing context (and its start time) must be kept.
    metrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    metrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    verify(operationTimer, times(1)).startTimer();
    verify(timingContext, times(1)).stopTimer();
  }

  @Test
  void tracksEachPhaseIndependently() {
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    // Completing one phase must not prevent a different phase from being measured.
    syncDurationMetrics.startTimer(Labels.TOTAL_SYNC_DURATION);
    syncDurationMetrics.stopTimer(Labels.TOTAL_SYNC_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).hasValue(1L);
    assertThat(observationCount(Labels.TOTAL_SYNC_DURATION)).hasValue(1L);
  }

  private Optional<Long> observationValue(final Labels label, final String type) {
    return metricsSystem
        .streamObservations()
        .filter(observation -> observation.labels().contains(label.name()))
        .filter(observation -> observation.labels().contains(type))
        .map(observation -> ((Number) observation.value()).longValue())
        .findFirst();
  }

  private Optional<Long> observationCount(final Labels label) {
    return observationValue(label, "count").filter(count -> count > 0);
  }
}
