/*
 * Copyright contributors to Hyperledger Besu.
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

import static java.util.Objects.requireNonNull;

import java.util.Collection;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;

class DebugMetricReader implements MetricReader {
  private @Nullable CollectionRegistration registration;

  public DebugMetricReader() {}

  public Collection<MetricData> getAllMetrics() {
    return requireNonNull(registration).collectAllMetrics();
  }

  @Override
  public void register(final @NotNull CollectionRegistration registration) {
    this.registration = registration;
  }

  @Override
  public CompletableResultCode forceFlush() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public AggregationTemporality getAggregationTemporality(
      final @NotNull InstrumentType instrumentType) {
    return AggregationTemporality.CUMULATIVE;
  }
}
