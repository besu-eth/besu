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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.services.pipeline.PipelineBuilder.createPipelineFrom;

import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.DynamicPivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.SnapDataRequest;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.request.v2.SnapV2BytecodeRequest;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.TaskQueueIterator;
import org.hyperledger.besu.ethereum.eth.sync.worldstate.WorldStateDownloadProcess;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.services.pipeline.Pipe;
import org.hyperledger.besu.services.pipeline.Pipeline;
import org.hyperledger.besu.services.pipeline.PipelineBuilder;
import org.hyperledger.besu.services.pipeline.WritePipe;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.util.ExceptionUtils;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static-pivot snap/2 leaf download pipeline. */
public class SnapV2WorldStateDownloadProcess implements WorldStateDownloadProcess {

  private static final Logger LOG = LoggerFactory.getLogger(SnapV2WorldStateDownloadProcess.class);

  private final Pipeline<Task<SnapDataRequest>> completionPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchAccountPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline;
  private final Pipeline<Task<SnapDataRequest>> fetchCodePipeline;
  private final WritePipe<Task<SnapDataRequest>> requestsToComplete;

  private SnapV2WorldStateDownloadProcess(
      final Pipeline<Task<SnapDataRequest>> fetchAccountPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline,
      final Pipeline<Task<SnapDataRequest>> fetchCodePipeline,
      final Pipeline<Task<SnapDataRequest>> completionPipeline,
      final WritePipe<Task<SnapDataRequest>> requestsToComplete) {
    this.fetchStorageDataPipeline = fetchStorageDataPipeline;
    this.fetchAccountPipeline = fetchAccountPipeline;
    this.fetchLargeStorageDataPipeline = fetchLargeStorageDataPipeline;
    this.fetchCodePipeline = fetchCodePipeline;
    this.completionPipeline = completionPipeline;
    this.requestsToComplete = requestsToComplete;
  }

  @Override
  public CompletableFuture<Void> start(final EthScheduler ethScheduler) {
    final CompletableFuture<Void> fetchAccountFuture =
        ethScheduler.startPipeline(fetchAccountPipeline);
    final CompletableFuture<Void> fetchStorageFuture =
        ethScheduler.startPipeline(fetchStorageDataPipeline);
    final CompletableFuture<Void> fetchLargeStorageFuture =
        ethScheduler.startPipeline(fetchLargeStorageDataPipeline);
    final CompletableFuture<Void> fetchCodeFuture = ethScheduler.startPipeline(fetchCodePipeline);
    final CompletableFuture<Void> completionFuture = ethScheduler.startPipeline(completionPipeline);

    fetchAccountFuture
        .thenCombine(fetchStorageFuture, (unused, unused2) -> null)
        .thenCombine(fetchLargeStorageFuture, (unused, unused2) -> null)
        .thenCombine(fetchCodeFuture, (unused, unused2) -> null)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                if (!(ExceptionUtils.rootCause(error) instanceof CancellationException)) {
                  LOG.error("Pipeline failed", error);
                }
                completionPipeline.abort();
              } else {
                requestsToComplete.close();
              }
            });

    completionFuture.exceptionally(
        error -> {
          if (!(ExceptionUtils.rootCause(error) instanceof CancellationException)) {
            LOG.error("Pipeline failed", error);
          }
          fetchAccountPipeline.abort();
          fetchStorageDataPipeline.abort();
          fetchLargeStorageDataPipeline.abort();
          fetchCodePipeline.abort();
          return null;
        });
    return completionFuture;
  }

  @Override
  public void abort() {
    fetchAccountPipeline.abort();
    fetchStorageDataPipeline.abort();
    fetchLargeStorageDataPipeline.abort();
    fetchCodePipeline.abort();
    completionPipeline.abort();
  }

  public static SnapV2WorldStateDownloadProcess create(
      final EthContext ethContext,
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final SnapSyncProcessState snapSyncState,
      final SnapV2WorldDownloadState downloadState,
      final DynamicPivotBlockSelector pivotBlockSelector,
      final SnapSyncConfiguration snapSyncConfiguration,
      final int maxOutstandingRequests,
      final MetricsSystem metricsSystem) {
    checkNotNull(ethContext);
    checkNotNull(worldStateStorageCoordinator);
    checkNotNull(snapSyncState);
    checkNotNull(downloadState);
    checkNotNull(pivotBlockSelector);
    checkNotNull(snapSyncConfiguration);
    checkNotNull(metricsSystem);

    final SnapV2RequestDataStep requestDataStep =
        new SnapV2RequestDataStep(
            ethContext, worldStateStorageCoordinator, snapSyncState, downloadState, metricsSystem);
    final SnapV2PersistDataStep persistDataStep =
        new SnapV2PersistDataStep(
            snapSyncState,
            worldStateStorageCoordinator,
            downloadState,
            snapSyncConfiguration,
            downloadState.getAccountRangeTracker(),
            downloadState.getStorageRangeTracker());
    final SnapV2CompleteTaskStep completeTaskStep =
        new SnapV2CompleteTaskStep(snapSyncState, metricsSystem);

    final int bufferCapacity = snapSyncConfiguration.getTrienodeCountPerRequest() * 2;
    final LabelledMetric<Counter> outputCounter =
        metricsSystem.createLabelledCounter(
            BesuMetricCategory.SYNCHRONIZER,
            "snap_v2_world_state_pipeline_processed_total",
            "Number of entries processed by each snap/2 world state download pipeline stage",
            "step",
            "action");

    final Pipeline<Task<SnapDataRequest>> completionPipeline =
        PipelineBuilder.<Task<SnapDataRequest>>createPipeline(
                "snapV2RequestDataAvailable",
                bufferCapacity,
                outputCounter,
                true,
                "node_data_request")
            .thenProcess(
                "snapV2CheckNewPivotBlock-Complete",
                task -> {
                  pivotBlockSelector.checkForNewPivotCandidate(downloadState::startPivotCatchup);
                  return task;
                })
            .andFinishWith(
                "snapV2RequestCompleteTask",
                task -> completeTaskStep.markAsCompleteOrFailed(downloadState, task));

    final Pipe<Task<SnapDataRequest>> requestsToComplete = completionPipeline.getInputPipe();

    final Pipeline<Task<SnapDataRequest>> fetchAccountDataPipeline =
        createPipelineFrom(
                "snapV2DequeueAccountRequestBlocking",
                new TaskQueueIterator<>(
                    downloadState, () -> downloadState.dequeueAccountRequestBlocking()),
                bufferCapacity,
                outputCounter,
                true,
                "world_state_download")
            .thenProcess(
                "snapV2CheckNewPivotBlock-Account",
                task -> {
                  pivotBlockSelector.checkForNewPivotCandidate(downloadState::startPivotCatchup);
                  return task;
                })
            .thenProcessAsync(
                "snapV2BatchDownloadAccountData",
                requestTask -> requestDataStep.requestAccount(requestTask),
                maxOutstandingRequests)
            .thenProcess("snapV2BatchPersistAccountData", task -> persistDataStep.persist(task))
            .andFinishWith("snapV2BatchAccountDataDownloaded", requestsToComplete::put);

    final Pipeline<Task<SnapDataRequest>> fetchStorageDataPipeline =
        createPipelineFrom(
                "snapV2DequeueStorageRequestBlocking",
                new TaskQueueIterator<>(
                    downloadState, () -> downloadState.dequeueStorageRequestBlocking()),
                bufferCapacity,
                outputCounter,
                true,
                "world_state_download")
            .inBatches(snapSyncConfiguration.getStorageCountPerRequest())
            .thenProcess(
                "snapV2CheckNewPivotBlock-Storage",
                tasks -> {
                  pivotBlockSelector.checkForNewPivotCandidate(downloadState::startPivotCatchup);
                  return tasks;
                })
            .thenProcessAsyncOrdered(
                "snapV2BatchDownloadStorageData",
                requestTask -> requestDataStep.requestStorage(requestTask),
                maxOutstandingRequests)
            .thenProcess("snapV2BatchPersistStorageData", task -> persistDataStep.persist(task))
            .andFinishWith(
                "snapV2BatchStorageDataDownloaded",
                tasks -> tasks.forEach(requestsToComplete::put));

    final Pipeline<Task<SnapDataRequest>> fetchLargeStorageDataPipeline =
        createPipelineFrom(
                "snapV2DequeueLargeStorageRequestBlocking",
                new TaskQueueIterator<>(
                    downloadState, () -> downloadState.dequeueLargeStorageRequestBlocking()),
                bufferCapacity,
                outputCounter,
                true,
                "world_state_download")
            .thenProcess(
                "snapV2CheckNewPivotBlock-LargeStorage",
                task -> {
                  pivotBlockSelector.checkForNewPivotCandidate(downloadState::startPivotCatchup);
                  return task;
                })
            .thenProcessAsyncOrdered(
                "snapV2BatchDownloadLargeStorageData",
                requestTask -> requestDataStep.requestStorage(List.of(requestTask)),
                maxOutstandingRequests)
            .thenProcess(
                "snapV2BatchPersistLargeStorageData",
                task -> {
                  persistDataStep.persist(task);
                  return task;
                })
            .andFinishWith(
                "snapV2BatchLargeStorageDataDownloaded",
                tasks -> tasks.forEach(requestsToComplete::put));

    final Pipeline<Task<SnapDataRequest>> fetchCodePipeline =
        createPipelineFrom(
                "snapV2DequeueCodeRequestBlocking",
                new TaskQueueIterator<>(
                    downloadState, () -> downloadState.dequeueCodeRequestBlocking()),
                bufferCapacity,
                outputCounter,
                true,
                "code_blocks_download_pipeline")
            .inBatches(
                snapSyncConfiguration.getBytecodeCountPerRequest() * 2,
                tasks ->
                    snapSyncConfiguration.getBytecodeCountPerRequest()
                        - (int)
                            tasks.stream()
                                .map(Task::getData)
                                .map(SnapV2BytecodeRequest.class::cast)
                                .map(SnapV2BytecodeRequest::getCodeHash)
                                .distinct()
                                .count())
            .thenProcess(
                "snapV2CheckNewPivotBlock-Code",
                tasks -> {
                  pivotBlockSelector.checkForNewPivotCandidate(downloadState::startPivotCatchup);
                  return tasks;
                })
            .thenProcessAsyncOrdered(
                "snapV2BatchDownloadCodeData",
                tasks -> requestDataStep.requestCode(tasks),
                maxOutstandingRequests)
            .thenProcess(
                "snapV2BatchPersistCodeData",
                tasks -> {
                  persistDataStep.persist(tasks);
                  return tasks;
                })
            .andFinishWith(
                "snapV2BatchCodeDataDownloaded", tasks -> tasks.forEach(requestsToComplete::put));

    return new SnapV2WorldStateDownloadProcess(
        fetchAccountDataPipeline,
        fetchStorageDataPipeline,
        fetchLargeStorageDataPipeline,
        fetchCodePipeline,
        completionPipeline,
        requestsToComplete);
  }
}
