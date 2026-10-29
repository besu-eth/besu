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

import io.vertx.core.VertxOptions;
import io.vertx.core.spi.VertxMetricsFactory;
import io.vertx.core.spi.metrics.VertxMetrics;

/** The Vertx metrics adapter factory. */
public class VertxMetricsAdapterFactory implements VertxMetricsFactory {

  private final MetricsSystem metricsSystem;
  private final String poolNamePrefix;

  /**
   * Instantiates a new Vertx metrics adapter factory.
   *
   * @param metricsSystem the metrics system
   */
  public VertxMetricsAdapterFactory(final MetricsSystem metricsSystem) {
    this(metricsSystem, "");
  }

  /**
   * Instantiates a new Vertx metrics adapter factory whose pool metrics are labeled with prefixed
   * pool names, so that pools of different Vertx instances are reported separately.
   *
   * @param metricsSystem the metrics system
   * @param poolNamePrefix the prefix prepended to each pool name
   */
  public VertxMetricsAdapterFactory(
      final MetricsSystem metricsSystem, final String poolNamePrefix) {
    this.metricsSystem = metricsSystem;
    this.poolNamePrefix = poolNamePrefix;
  }

  @Override
  public VertxMetrics metrics(final VertxOptions options) {
    return new VertxMetricsAdapter(metricsSystem, poolNamePrefix);
  }
}
