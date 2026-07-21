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
package org.hyperledger.besu.ethereum.eth.sync.snapsync.v2;

import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldDownloadState;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.services.tasks.Task;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** snap/2 task completion step. Manages completion status. */
public class SnapV2CompleteTaskStep {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2CompleteTaskStep.class);

  private static final long REQUEUE_LOG_INTERVAL_MS = 30_000L;

  private final SnapSyncProcessState snapSyncState;
  private final Counter completedRequestsCounter;
  private final Counter retriedRequestsCounter;
  private final AtomicLong requeueCount = new AtomicLong();
  private final AtomicLong lastRequeueLogMillis = new AtomicLong();

  public SnapV2CompleteTaskStep(
      final SnapSyncProcessState snapSyncState, final MetricsSystem metricsSystem) {
    this.snapSyncState = snapSyncState;
    completedRequestsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_completed_requests_total",
            "Total number of node data requests completed as part of snap/2 world state download");
    retriedRequestsCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_retried_requests_total",
            "Total number of node data requests repeated as part of snap/2 world state download");
  }

  public synchronized void markAsCompleteOrFailed(
      final WorldDownloadState<SnapDataRequest> downloadState, final Task<SnapDataRequest> task) {
    final SnapDataRequest request = task.getData();
    if (request.isExpired(snapSyncState)) {
      throw new IllegalStateException(expiredRequestMessage(request));
    }
    if (request.isResponseReceived()) {
      completedRequestsCounter.inc();
      task.markCompleted();
      downloadState.checkCompletion(snapSyncState.getPivotBlockHeader().orElseThrow());
    } else {
      retriedRequestsCounter.inc();
      logRequeue(request);
      task.markFailed();
    }
    downloadState.notifyTaskAvailable();
  }

  private String expiredRequestMessage(final SnapDataRequest request) {
    final String currentPivot =
        snapSyncState.getPivotBlockHeader().map(header -> header.toLogString()).orElse("empty");
    final String requestPivot =
        request instanceof SnapV2DataRequest snapV2DataRequest
            ? snapV2DataRequest.getPivotBlockHeader().toLogString()
            : "unknown";
    return "Expired snap/2 request reached completion step: type "
        + request.getRequestType()
        + ", request pivot "
        + requestPivot
        + ", current pivot "
        + currentPivot;
  }

  private void logRequeue(final SnapDataRequest request) {
    requeueCount.incrementAndGet();
    final long now = System.currentTimeMillis();
    final long last = lastRequeueLogMillis.get();
    if (now - last >= REQUEUE_LOG_INTERVAL_MS && lastRequeueLogMillis.compareAndSet(last, now)) {
      final long loggedCount = requeueCount.getAndSet(0);
      LOG.warn(
          "snap/2 requeued {} request(s) in the last {}s (most recent type: {}): "
              + "no valid response — peer may have returned empty data, sent invalid proof, "
              + "or the request timed out",
          loggedCount,
          REQUEUE_LOG_INTERVAL_MS / 1000,
          request.getRequestType());
    }
  }
}
