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
package org.hyperledger.besu.metrics.vertx;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import io.vertx.core.spi.metrics.PoolMetrics;

// Vert.x 5's PoolMetrics<Q, T> brackets two phases: enqueue()/dequeue(Q) around queueing, and
// begin()/end(T) around execution. There is no "rejected" callback in the new SPI (the old
// vertx_worker_pool_rejected_total metric has no signal to feed it and is dropped), and end(T) no
// longer carries a success flag -- the old adapter incremented completedCounter unconditionally
// regardless of that flag anyway, so this preserves the same observable counter behavior.
final class PoolMetricsAdapter implements PoolMetrics<Object, Object> {

  private static final Object TASK_CONTEXT = new Object();

  private final Counter submittedCounter;
  private final Counter completedCounter;

  public PoolMetricsAdapter(
      final MetricsSystem metricsSystem, final String poolType, final String poolName) {
    submittedCounter =
        metricsSystem
            .createLabelledCounter(
                BesuMetricCategory.NETWORK,
                "vertx_worker_pool_submitted_total",
                "Total number of tasks submitted to the Vertx worker pool",
                "poolType",
                "poolName")
            .labels(poolType, poolName);

    completedCounter =
        metricsSystem
            .createLabelledCounter(
                BesuMetricCategory.NETWORK,
                "vertx_worker_pool_completed_total",
                "Total number of tasks completed by the Vertx worker pool",
                "poolType",
                "poolName")
            .labels(poolType, poolName);
  }

  @Override
  public Object enqueue() {
    submittedCounter.inc();
    return TASK_CONTEXT;
  }

  @Override
  public Object begin() {
    return TASK_CONTEXT;
  }

  @Override
  public void end(final Object o) {
    completedCounter.inc();
  }
}
