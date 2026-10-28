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

import org.hyperledger.besu.plugin.services.metrics.LabelledSuppliedMetric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.DoubleSupplier;

import com.google.common.base.Preconditions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

abstract class OpenTelemetryLabelledSuppliedMetric implements LabelledSuppliedMetric {
  private final List<String> labelNames;
  private final Map<Attributes, DoubleSupplier> observationsMap = new ConcurrentHashMap<>();

  public OpenTelemetryLabelledSuppliedMetric(final List<String> labelNames) {
    this.labelNames = labelNames;
  }

  @Override
  public void labels(final DoubleSupplier valueSupplier, final String... labelValues) {
    Preconditions.checkArgument(
        labelValues.length == labelNames.size(),
        "label values and label names need the same number of elements");
    final Attributes labels = getLabels(labelValues);
    if (observationsMap.putIfAbsent(labels, valueSupplier) != null) {
      throw new IllegalStateException(
          "Already registered a collector with label values " + Arrays.toString(labelValues));
    }
  }

  private Attributes getLabels(final String... labelValues) {
    final AttributesBuilder labelsBuilder = Attributes.builder();
    for (int i = 0; i < labelNames.size(); i++) {
      labelsBuilder.put(labelNames.get(i), labelValues[i]);
    }
    return labelsBuilder.build();
  }

  /**
   * Callback to record the supplied values
   *
   * @param measurement where to record the values
   */
  protected void updater(final ObservableDoubleMeasurement measurement) {
    observationsMap.forEach(
        (labels, valueSupplier) -> measurement.record(valueSupplier.getAsDouble(), labels));
  }

  /** Stops reporting the metric, as when it is created again. */
  abstract void close();
}
