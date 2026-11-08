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
package org.hyperledger.besu.metrics.opentelemetry;

import java.util.List;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;

/** The Open telemetry supplied counter. */
public class OpenTelemetrySuppliedCounter extends OpenTelemetryLabelledSuppliedMetric {
  private final ObservableDoubleCounter counter;

  /**
   * Instantiates a new Open telemetry supplied counter.
   *
   * @param metricName the metric name
   * @param help the help
   * @param meter the meter
   * @param labelNames the label names
   */
  public OpenTelemetrySuppliedCounter(
      final String metricName,
      final String help,
      final Meter meter,
      final List<String> labelNames) {
    super(labelNames);
    counter =
        meter
            .counterBuilder(metricName)
            .setDescription(help)
            .ofDoubles()
            .buildWithCallback(this::updater);
  }

  @Override
  void close() {
    counter.close();
  }
}
