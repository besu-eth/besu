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

import org.hyperledger.besu.metrics.SyncDurationMetrics.Labels;
import org.hyperledger.besu.metrics.prometheus.PrometheusMetricsSystem;

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
  void restartingARunningPhaseKeepsTheOriginalStartTime() throws InterruptedException {
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    Thread.sleep(50);
    // A re-pivot creates a new chain downloader which starts the timer again while the previous
    // measurement is still running: the elapsed time so far must not be discarded.
    syncDurationMetrics.startTimer(Labels.CHAIN_DOWNLOAD_DURATION);
    syncDurationMetrics.stopTimer(Labels.CHAIN_DOWNLOAD_DURATION);

    assertThat(observationCount(Labels.CHAIN_DOWNLOAD_DURATION)).hasValue(1L);
    assertThat(observationSum(Labels.CHAIN_DOWNLOAD_DURATION))
        .hasValueSatisfying(sum -> assertThat(sum).isGreaterThanOrEqualTo(0.04));
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

  private Optional<Double> observationSum(final Labels label) {
    return metricsSystem
        .streamObservations()
        .filter(observation -> observation.labels().contains(label.name()))
        .filter(observation -> observation.labels().contains("sum"))
        .map(observation -> ((Number) observation.value()).doubleValue())
        .findFirst();
  }
}
