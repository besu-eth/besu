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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;

class GetBlobsMetrics {
  private final LabelledMetric<Counter> requestedCounter;
  private final LabelledMetric<Counter> availableCounter;
  private final LabelledMetric<Counter> missingCounter;
  private final LabelledMetric<Counter> unsupportedCounter;
  private final LabelledMetric<Counter> fullCounter;
  private final LabelledMetric<Counter> partialCounter;
  private final String version;

  public GetBlobsMetrics(final MetricsSystem metricsSystem, final int version) {
    this.version = String.valueOf(version);
    this.requestedCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_requested_total",
            "Number of blobs requested via engine_getBlobsV*",
            "version");
    this.availableCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_available_total",
            "Number of blobs requested via engine_getBlobsV* that are present in the blob pool",
            "version");
    this.missingCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_missing_total",
            "Number of blobs requested via engine_getBlobsV* that are not present in the blob pool",
            "version");
    this.unsupportedCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_unsupported_total",
            "Number of blobs requested via engine_getBlobsV* that have unsupported type",
            "version");
    this.fullCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_full_total",
            "Number of calls to engine_getBlobsV* that returned all blobs",
            "version");
    this.partialCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_partial_total",
            "Number of calls to engine_getBlobsV* that returned partial responses",
            "version");
  }

  public void increaseRequested(final int count) {
    requestedCounter.labels(version).inc(count);
  }

  public void increaseAvailable(final long availableCount) {
    availableCounter.labels(version).inc(availableCount);
  }

  public void increaseUnsupported(final int unsupportedBlobs) {
    unsupportedCounter.labels(version).inc(unsupportedBlobs);
  }

  public void increaseMissing(final int missingBlobs) {
    missingCounter.labels(version).inc(missingBlobs);
  }

  public void increaseFull() {
    fullCounter.labels(version).inc();
  }

  public void increasePartial() {
    partialCounter.labels(version).inc();
  }
}
