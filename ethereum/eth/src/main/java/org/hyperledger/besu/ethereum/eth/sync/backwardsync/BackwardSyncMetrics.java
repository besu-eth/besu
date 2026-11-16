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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/** Metrics of the backward sync. */
class BackwardSyncMetrics {
  private final Counter importedExecuted;
  private final Counter importedFromBal;
  private final Counter balApplyFallbacks;

  BackwardSyncMetrics(
      final MetricsSystem metricsSystem,
      final LongSupplier pendingHeaders,
      final LongSupplier targetBlock,
      final IntSupplier windowBlocks,
      final LongSupplier windowBytes) {
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "backward_sync_pending_headers",
        "Blocks between the local head and the top of the backward sync chain",
        pendingHeaders);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "backward_sync_target_block",
        "Number of the block the backward sync is heading to",
        targetBlock);
    metricsSystem.createIntegerGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "backward_sync_window_blocks",
        "Blocks held in the backward sync download window",
        windowBlocks);
    metricsSystem.createLongGauge(
        BesuMetricCategory.SYNCHRONIZER,
        "backward_sync_window_bytes",
        "Bytes held in the backward sync download window",
        windowBytes);
    final LabelledMetric<Counter> imported =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "backward_sync_imported_blocks_total",
            "Blocks imported by the backward sync",
            "method");
    importedExecuted = imported.labels("executed");
    importedFromBal = imported.labels("bal");
    balApplyFallbacks =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "backward_sync_bal_apply_fallbacks_total",
            "Blocks that could not be applied from their block access list and were executed");
  }

  void imported(final boolean fromBal) {
    (fromBal ? importedFromBal : importedExecuted).inc();
  }

  void balApplyFallback() {
    balApplyFallbacks.inc();
  }
}
