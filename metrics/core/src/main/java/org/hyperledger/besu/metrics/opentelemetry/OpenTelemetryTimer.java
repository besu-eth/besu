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

import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.metrics.OperationTimer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;

/** The Open telemetry timer. */
public class OpenTelemetryTimer implements LabelledMetric<OperationTimer> {

  private final String[] labelNames;
  private final ConcurrentHashMap<Attributes, AtomicLong> elapsedNanosMap =
      new ConcurrentHashMap<>();

  /**
   * Instantiates a new Open telemetry timer.
   *
   * @param metricName the metric name
   * @param help the help
   * @param meter the meter
   * @param labelNames the label names
   */
  public OpenTelemetryTimer(
      final String metricName, final String help, final Meter meter, final String... labelNames) {
    this.labelNames = labelNames;
    // Register a single permanent callback; it reads the latest elapsed time for each label set.
    meter
        .gaugeBuilder(metricName)
        .setDescription(help)
        .buildWithCallback(
            measurement ->
                elapsedNanosMap.forEach(
                    (attrs, nanos) -> measurement.record(nanos.get() / 1e9, attrs)));
  }

  @Override
  public OperationTimer labels(final String... labelValues) {
    final AttributesBuilder builder = Attributes.builder();
    for (int i = 0; i < labelNames.length; i++) {
      builder.put(labelNames[i], labelValues[i]);
    }
    final Attributes labels = builder.build();
    // Lazily create the AtomicLong slot for this label set on first stop().
    return () -> {
      final long startTime = System.nanoTime();
      return () -> {
        final long elapsed = System.nanoTime() - startTime;
        elapsedNanosMap.computeIfAbsent(labels, k -> new AtomicLong()).set(elapsed);
        return elapsed / 1e9;
      };
    };
  }
}
