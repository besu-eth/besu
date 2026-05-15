/*
 * Copyright contributors to Hyperledger Besu.
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

import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages the synchronization duration metrics for the Hyperledger Besu project. It
 * provides methods to start and stop timers for various synchronization phases.
 */
public class SyncDurationMetrics {

  /** A {@link SyncDurationMetrics} instance that does not record any metrics. */
  public static final SyncDurationMetrics NO_OP_SYNC_DURATION_METRICS =
      new SyncDurationMetrics(new NoOpMetricsSystem());

  private final LabelledMetric<OperationTimer> timer;

  private final Map<String, OperationTimer.TimingContext> timers = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link SyncDurationMetrics} instance.
   *
   * @param metricsSystem The {@link MetricsSystem} to use to record metrics.
   */
  public SyncDurationMetrics(final MetricsSystem metricsSystem) {
    timer =
        metricsSystem.createSimpleLabelledTimer(
            BesuMetricCategory.SYNCHRONIZER, "sync_duration", "Time taken to sync", "name");
  }

  /**
   * Starts a timer for the given synchronization phase.
   *
   * @param label The synchronization phase to start the timer for.
   */
  public void startTimer(final Labels label) {
    timers.computeIfAbsent(label.name(), k -> timer.labels(label.name()).startTimer());
  }

  /**
   * Stops the timer for the given synchronization phase.
   *
   * @param label The synchronization phase to stop the timer for.
   */
  public void stopTimer(final Labels label) {
    OperationTimer.TimingContext context = timers.remove(label.name());
    if (context != null) {
      context.stopTimer();
    }
  }

  /** Enum representing the different synchronization phases. */
  public enum Labels {
    /**
     * Total time taken to get into sync. It is useful for SNAP sync mode only.
     *
     * <p>Total sync duration includes the separate stages mentioned below, some of which occur in
     * parallel.
     *
     * <p>Total sync duration excludes the backwards sync stage due to implementation challenges.
     * The backwards sync should be a very short duration following the other sync stages.
     */
    TOTAL_SYNC_DURATION,
    /** Time taken to download the chain data (headers, blocks, receipts). */
    CHAIN_DOWNLOAD_DURATION,
    /** Time taken to download the initial world state, before the healing step. */
    SNAP_INITIAL_WORLD_STATE_DOWNLOAD_DURATION,
    /** Time taken to heal the world state, after the initial download. */
    SNAP_WORLD_STATE_HEALING_DURATION,
    /** Time taken to do the flat database heal. */
    FLAT_DB_HEAL;
  }
}
