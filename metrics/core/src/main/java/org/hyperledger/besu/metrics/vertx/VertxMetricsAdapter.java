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

import org.hyperledger.besu.plugin.services.MetricsSystem;

import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;

/** The Vertx metrics adapter. */
public class VertxMetricsAdapter implements VertxMetrics {

  private final MetricsSystem metricsSystem;

  /**
   * Instantiates a new Vertx metrics adapter.
   *
   * @param metricsSystem the metrics system
   */
  public VertxMetricsAdapter(final MetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;
  }

  @Override
  public PoolMetrics<?, ?> createPoolMetrics(
      final String poolType, final String poolName, final int maxPoolSize) {
    return new PoolMetricsAdapter(metricsSystem, poolType, poolName);
  }
}
