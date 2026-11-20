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
package org.hyperledger.besu.metrics.vertx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.metrics.BesuMetricCategory.DEFAULT_METRIC_CATEGORIES;
import static org.hyperledger.besu.metrics.BesuMetricCategory.NETWORK;

import org.hyperledger.besu.metrics.Observation;
import org.hyperledger.besu.metrics.prometheus.PrometheusMetricsSystem;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VertxMetricsAdapterTest {

  private static final String SUBMITTED = "vertx_worker_pool_submitted_total";

  private PrometheusMetricsSystem metricsSystem;

  @BeforeEach
  public void setUp() {
    metricsSystem = new PrometheusMetricsSystem(DEFAULT_METRIC_CATEGORIES, true);
  }

  @AfterEach
  public void tearDown() {
    metricsSystem.shutdown();
  }

  @Test
  public void poolsWithTheSameNameAreReportedSeparatelyWhenPrefixed() {
    new VertxMetricsAdapter(metricsSystem)
        .createPoolMetrics("worker", "vert.x-worker-thread", 20)
        .enqueue();
    new VertxMetricsAdapter(metricsSystem, "engine-api-")
        .createPoolMetrics("worker", "vert.x-worker-thread", 8)
        .enqueue();

    assertThat(
            metricsSystem.streamObservations(NETWORK).filter(o -> o.metricName().equals(SUBMITTED)))
        .containsExactlyInAnyOrder(
            new Observation(NETWORK, SUBMITTED, 1.0, List.of("vert.x-worker-thread", "worker")),
            new Observation(
                NETWORK, SUBMITTED, 1.0, List.of("engine-api-vert.x-worker-thread", "worker")));
  }
}
