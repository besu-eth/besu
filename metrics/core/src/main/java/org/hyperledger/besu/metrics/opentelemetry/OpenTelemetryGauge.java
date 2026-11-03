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
package org.hyperledger.besu.metrics.opentelemetry;

import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;

import java.util.List;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;

/** The Open telemetry gauge. */
public class OpenTelemetryGauge extends OpenTelemetryLabelledSuppliedMetric
    implements LabelledSuppliedMetric {
  private final ObservableDoubleGauge gauge;

  /**
   * Instantiates a new Open telemetry gauge.
   *
   * @param metricName the metric name
   * @param help the help
   * @param meter the meter
   * @param labelNames the label names
   */
  public OpenTelemetryGauge(
      final String metricName,
      final String help,
      final Meter meter,
      final List<String> labelNames) {
    super(labelNames);

    gauge = meter.gaugeBuilder(metricName).setDescription(help).buildWithCallback(this::updater);
  }

  @Override
  void close() {
    gauge.close();
  }
}
