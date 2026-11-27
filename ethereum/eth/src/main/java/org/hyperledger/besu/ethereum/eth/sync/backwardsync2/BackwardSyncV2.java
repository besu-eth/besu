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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync2;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.manager.EthContext;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BackwardSync;
import org.hyperledger.besu.ethereum.eth.sync.backwardsync.BadChainListener;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.util.Subscribers;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Backward sync on the Besu pipelines. While the chain does not hold the latest head the consensus
 * client sent, it syncs to that head, one {@link HeadSync} at a time.
 */
public class BackwardSyncV2 implements BackwardSync {

  private static final Logger LOG = LoggerFactory.getLogger(BackwardSyncV2.class);

  // the newest blocks may not be served by peers yet, they are imported from their new payload
  private static final int RECENT_PAYLOADS = 64;

  private final ProtocolContext protocolContext;
  private final EthContext ethContext;
  private final HeadSync headSync;
  private final Subscribers<BadChainListener> badChainListeners = Subscribers.create();
  private final AtomicReference<Hash> finalizedBlockHash = new AtomicReference<>(Hash.ZERO);

  // guarded by this
  private final Map<Hash, Head> recentPayloads =
      new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Hash, Head> eldest) {
          return size() > RECENT_PAYLOADS;
        }
      };
  private Head latestHead;
  private boolean running;
  // the reason the last sync stopped, null once a sync succeeds
  private String lastFailure;
  private CompletableFuture<Void> sync = CompletableFuture.completedFuture(null);

  public BackwardSyncV2(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final SynchronizerConfiguration syncConfig,
      final MetricsSystem metricsSystem,
      final EthContext ethContext,
      final StorageProvider storageProvider) {
    this.protocolContext = protocolContext;
    this.ethContext = ethContext;
    this.headSync =
        new HeadSync(
            protocolContext,
            protocolSchedule,
            syncConfig,
            metricsSystem,
            ethContext,
            new HeaderStore(
                storageProvider.getStorageBySegmentIdentifier(
                    KeyValueSegmentIdentifier.BACKWARD_SYNC_HEADERS),
                ScheduleBasedBlockHeaderFunctions.create(protocolSchedule)),
            finalizedBlockHash::get,
            this::recentPayload,
            badChainListeners);
  }

  @Override
  public synchronized boolean isSyncing() {
    return running;
  }

  // the progress logs use the head of each sync
  @Override
  public void maybeUpdateTargetHeight(final Hash headHash) {}

  @Override
  public void updateFinalizedBlockHash(final Hash finalizedBlockHash) {
    this.finalizedBlockHash.set(finalizedBlockHash);
  }

  @Override
  public synchronized CompletableFuture<Void> syncBackwardsUntil(final Hash newBlockHash) {
    latestHead = recentPayloads.getOrDefault(newBlockHash, Head.ofHash(newBlockHash));
    return startIfIdle();
  }

  @Override
  public synchronized CompletableFuture<Void> syncBackwardsUntil(
      final Block newPivot, final Optional<BlockAccessList> blockAccessList) {
    latestHead = Head.ofPayload(newPivot, blockAccessList);
    recentPayloads.put(latestHead.hash(), latestHead);
    return startIfIdle();
  }

  // the headers a sync downloads are stored by number, only the new payloads are found by hash
  @Override
  public synchronized Optional<BlockHeader> getHeader(final Hash blockHash) {
    return Optional.ofNullable(recentPayloads.get(blockHash))
        .flatMap(Head::block)
        .map(Block::getHeader);
  }

  private synchronized Optional<Head> recentPayload(final Hash blockHash) {
    return Optional.ofNullable(recentPayloads.get(blockHash));
  }

  @Override
  public void subscribeBadChainListener(final BadChainListener badChainListener) {
    badChainListeners.subscribe(badChainListener);
  }

  private CompletableFuture<Void> startIfIdle() {
    if (!running) {
      running = true;
      LOG.info("Starting backward sync");
      sync = ethContext.getScheduler().scheduleServiceTask(this::syncToLatestHead);
    }
    return sync;
  }

  private void syncToLatestHead() {
    try {
      for (Optional<Head> next = nextHead(); next.isPresent(); next = nextHead()) {
        headSync.syncTo(next.get());
      }
      LOG.info("Backward sync done");
    } catch (final RuntimeException | Error e) {
      stoppedBy(e);
      throw e;
    }
  }

  /** Logs a failure once while every head that restarts the sync fails the same way. */
  private synchronized void stoppedBy(final Throwable failure) {
    running = false;
    final String reason = String.valueOf(failure.getMessage());
    LOG.atLevel(reason.equals(lastFailure) ? Level.DEBUG : Level.INFO)
        .setMessage(
            "Backward sync stopped ({}), the next head from the consensus client restarts it")
        .addArgument(reason)
        .log();
    LOG.debug("Backward sync failure", failure);
    lastFailure = reason;
  }

  /** The head to sync to next, empty once the chain holds the latest one, the sync then done. */
  private synchronized Optional<Head> nextHead() {
    if (protocolContext.getBlockchain().contains(latestHead.hash())) {
      running = false;
      lastFailure = null;
      recentPayloads.clear();
      return Optional.empty();
    }
    return Optional.of(latestHead);
  }
}
