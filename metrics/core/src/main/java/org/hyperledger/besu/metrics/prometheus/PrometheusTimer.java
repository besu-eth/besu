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
package org.hyperledger.besu.metrics.prometheus;

import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.Map;

import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.metrics.Summary;

/**
 * An implementation of Besu timer backed by a Prometheus summary. The summary provides a total
 * count of durations and a sum of all observed durations, it calculates configurable quantiles over
 * a sliding time window.
 */
class PrometheusTimer extends AbstractPrometheusSummary implements LabelledMetric<OperationTimer> {

  public PrometheusTimer(
      final MetricCategory category,
      final String name,
      final String help,
      final Map<Double, Double> quantiles,
      final String... labelNames) {
    super(category, name);
    final var summaryBuilder =
        Summary.builder().name(this.prefixedName).help(help).labelNames(labelNames);
    quantiles.forEach(summaryBuilder::quantile);
    this.collector = summaryBuilder.build();
  }

  @Override
  public OperationTimer labels(final String... labels) {
    final DistributionDataPoint metric = ((Summary) collector()).labelValues(labels);
    return () -> metric.startTimer()::observeDuration;
  }
}
