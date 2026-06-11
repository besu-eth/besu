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
package org.hyperledger.besu.ethereum.eth.sync.worldstate;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.manager.task.EthTask;
import org.hyperledger.besu.ethereum.worldstate.WorldStateStorageCoordinator;
import org.hyperledger.besu.metrics.SyncDurationMetrics;
import org.hyperledger.besu.services.tasks.InMemoryTasksPriorityQueues;
import org.hyperledger.besu.services.tasks.Task;
import org.hyperledger.besu.services.tasks.TasksPriorityProvider;
import org.hyperledger.besu.util.ExceptionUtils;

import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WorldDownloadState<REQUEST extends TasksPriorityProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(WorldDownloadState.class);
  protected final SyncDurationMetrics syncDurationMetrics;

  private boolean downloadWasResumed;
  protected final InMemoryTasksPriorityQueues<REQUEST> pendingRequests;

  protected final int maxRequestsWithoutProgress;
  private final Clock clock;
  private final Set<EthTask<?>> outstandingRequests =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected CompletableFuture<Void> internalFuture;
  private CompletableFuture<Void> downloadFuture;
  // Volatile so monitoring can access it without having to synchronize.
  protected volatile int requestsSinceLastProgress = 0;
  private final long minMillisBeforeStalling;
  private volatile long timestampOfLastProgress;
  protected Bytes rootNodeData;

  protected final WorldStateStorageCoordinator worldStateStorageCoordinator;
  protected WorldStateDownloadProcess worldStateDownloadProcess;

  public WorldDownloadState(
      final WorldStateStorageCoordinator worldStateStorageCoordinator,
      final InMemoryTasksPriorityQueues<REQUEST> pendingRequests,
      final int maxRequestsWithoutProgress,
      final long minMillisBeforeStalling,
      final Clock clock,
      final SyncDurationMetrics syncDurationMetrics) {
    this.worldStateStorageCoordinator = worldStateStorageCoordinator;
    this.minMillisBeforeStalling = minMillisBeforeStalling;
    this.timestampOfLastProgress = clock.millis();
    this.downloadWasResumed = !pendingRequests.isEmpty();
    this.pendingRequests = pendingRequests;
    this.maxRequestsWithoutProgress = maxRequestsWithoutProgress;
    this.clock = clock;
    this.syncDurationMetrics = syncDurationMetrics;
    this.internalFuture = new CompletableFuture<>();
    this.downloadFuture = new CompletableFuture<>();
    this.internalFuture.whenComplete(this::cleanup);
    this.downloadFuture.exceptionally(
        error -> {
          // Propagate cancellation back to our internal future.
          if (error instanceof CancellationException) {
            this.internalFuture.cancel(true);
          }
          return null;
        });
  }

  public void reset() {
    this.timestampOfLastProgress = clock.millis();
    this.requestsSinceLastProgress = 0;
    this.downloadWasResumed = true;
    this.internalFuture = new CompletableFuture<>();
    this.downloadFuture = new CompletableFuture<>();
    this.internalFuture.whenComplete(this::cleanup);
    this.downloadFuture.exceptionally(
        error -> {
          // Propagate cancellation back to our internal future.
          if (error instanceof CancellationException) {
            this.internalFuture.cancel(true);
          }
          return null;
        });
  }

  private synchronized void cleanup(final Void result, final Throwable error) {
    // Handle cancellations
    if (internalFuture.isCancelled()) {
      LOG.info("World state download cancelled");
    } else if (error != null) {
      if (!(ExceptionUtils.rootCause(error) instanceof StalledDownloadException)) {
        LOG.info("World state download failed. ", error);
      }
    }

    cleanupQueues();

    if (error != null) {
      if (worldStateDownloadProcess != null) {
        worldStateDownloadProcess.abort();
      }
      downloadFuture.completeExceptionally(error);
    } else {
      downloadFuture.complete(result);
    }
  }

  protected synchronized void cleanupQueues() {
    for (final EthTask<?> outstandingRequest : outstandingRequests) {
      outstandingRequest.cancel();
    }
    pendingRequests.clear();
  }

  public boolean downloadWasResumed() {
    return downloadWasResumed;
  }

  public void addOutstandingTask(final EthTask<?> task) {
    outstandingRequests.add(task);
  }

  public void removeOutstandingTask(final EthTask<?> task) {
    outstandingRequests.remove(task);
  }

  public int getOutstandingTaskCount() {
    return outstandingRequests.size();
  }

  public CompletableFuture<Void> getDownloadFuture() {
    return downloadFuture;
  }

  public synchronized void enqueueRequest(final REQUEST request) {
    if (!internalFuture.isDone()) {
      pendingRequests.add(request);
      notifyAll();
    }
  }

  public synchronized void enqueueRequests(final Stream<REQUEST> requests) {
    if (!internalFuture.isDone()) {
      requests.forEach(pendingRequests::add);
      notifyAll();
    }
  }

  public synchronized Task<REQUEST> dequeueRequestBlocking() {
    while (!internalFuture.isDone()) {
      Task<REQUEST> task = pendingRequests.remove();
      if (task != null) {
        return task;
      }
      try {
        wait();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
    return null;
  }

  public synchronized void setRootNodeData(final Bytes rootNodeData) {
    this.rootNodeData = rootNodeData;
  }

  public synchronized void notePivotSwitch() {
    requestsSinceLastProgress = 0;
    timestampOfLastProgress = clock.millis();
  }

  public synchronized void requestComplete(
      final boolean madeProgress, final long minMillisBeforeStalling) {
    if (madeProgress) {
      requestsSinceLastProgress = 0;
      timestampOfLastProgress = clock.millis();
    } else {
      requestsSinceLastProgress++;
      if (requestsSinceLastProgress >= maxRequestsWithoutProgress
          && timestampOfLastProgress + minMillisBeforeStalling < clock.millis()) {
        markAsStalled(maxRequestsWithoutProgress);
      }
    }
  }

  public synchronized void requestComplete(final boolean madeProgress) {
    requestComplete(madeProgress, minMillisBeforeStalling);
  }

  public int getRequestsSinceLastProgress() {
    return requestsSinceLastProgress;
  }

  protected synchronized void markAsStalled(final int maxNodeRequestRetries) {
    final String message =
        "Download stalled due to too many failures to retrieve node data (>"
            + maxNodeRequestRetries
            + " requests without making progress)";

    final WorldStateDownloaderException e = new StalledDownloadException(message);
    internalFuture.completeExceptionally(e);
  }

  public synchronized boolean isDownloading() {
    return !internalFuture.isDone();
  }

  public synchronized void notifyTaskAvailable() {
    notifyAll();
  }

  public CompletableFuture<Void> startDownload(
      final WorldStateDownloadProcess worldStateDownloadProcess, final EthScheduler ethScheduler) {
    this.worldStateDownloadProcess = worldStateDownloadProcess;
    final CompletableFuture<Void> processFuture = worldStateDownloadProcess.start(ethScheduler);

    processFuture.whenComplete(
        (result, error) -> {
          if (error != null
              && !(ExceptionUtils.rootCause(error) instanceof CancellationException)) {
            // The pipeline is only ever cancelled by us or shutdown closing the EthScheduler
            // In either case we don't want to consider the download failed as we either already
            // dealing with it or it's just a normal shutdown. Hence, don't propagate
            // CancellationException
            internalFuture.completeExceptionally(error);
          }
        });
    return downloadFuture;
  }

  public void setWorldStateDownloadProcess(
      final WorldStateDownloadProcess worldStateDownloadProcess) {
    this.worldStateDownloadProcess = worldStateDownloadProcess;
  }

  public abstract boolean checkCompletion(final BlockHeader header);
}
