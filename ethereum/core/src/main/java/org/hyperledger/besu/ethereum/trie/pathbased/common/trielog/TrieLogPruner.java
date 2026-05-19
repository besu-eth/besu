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
package org.hyperledger.besu.ethereum.trie.pathbased.common.trielog;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriority;
import org.hyperledger.besu.plugin.services.storage.StorageReadPriorityContext;
import org.hyperledger.besu.plugin.services.trielogs.TrieLogEvent;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrieLogPruner implements TrieLogEvent.TrieLogObserver {

  private static final Logger LOG = LoggerFactory.getLogger(TrieLogPruner.class);
  private static final int PRELOAD_TIMEOUT_IN_SECONDS = 30;

  private final int pruningLimit;
  private final int loadingLimit;
  private final PathBasedWorldStateKeyValueStorage rootWorldStateStorage;
  private final Blockchain blockchain;
  private final Consumer<Runnable> executeAsync;
  private final long numBlocksToRetain;
  private final boolean requireFinalizedBlock;
  private final Counter addedToPruneQueueCounter;
  private final Counter prunedFromQueueCounter;
  private final Counter prunedOrphanCounter;

  private final Multimap<Long, Hash> trieLogBlocksAndForksByDescendingBlockNumber =
      TreeMultimap.create(
          Comparator.reverseOrder(),
          Comparator.comparing(Hash::getBytes, Comparator.naturalOrder()));

  public TrieLogPruner(
      final PathBasedWorldStateKeyValueStorage rootWorldStateStorage,
      final Blockchain blockchain,
      final Consumer<Runnable> executeAsync,
      final long numBlocksToRetain,
      final int pruningLimit,
      final boolean requireFinalizedBlock,
      final MetricsSystem metricsSystem) {
    this.rootWorldStateStorage = rootWorldStateStorage;
    this.blockchain = blockchain;
    this.executeAsync = executeAsync;
    this.numBlocksToRetain = numBlocksToRetain;
    this.pruningLimit = pruningLimit;
    this.loadingLimit = pruningLimit; // same as pruningLimit for now
    this.requireFinalizedBlock = requireFinalizedBlock;
    this.addedToPruneQueueCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PRUNER,
            "trie_log_added_to_prune_queue",
            "trie log added to prune queue");
    this.prunedFromQueueCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PRUNER, "trie_log_pruned_from_queue", "trie log pruned from queue");
    this.prunedOrphanCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.PRUNER, "trie_log_pruned_orphan", "trie log pruned orphan");
  }

  public void initialize() {
    preloadQueueWithTimeout(PRELOAD_TIMEOUT_IN_SECONDS);
  }

  @VisibleForTesting
  void preloadQueueWithTimeout(final int timeoutInSeconds) {

    LOG.info("Trie log pruner queue preload starting...");
    LOG.atInfo()
        .setMessage("Attempting to load first {} trie logs from database...")
        .addArgument(loadingLimit)
        .log();

    try (final ExecutorService preloadExecutor = Executors.newSingleThreadExecutor()) {
      final Future<?> future =
          preloadExecutor.submit(
              () ->
                  StorageReadPriorityContext.withPriority(
                      StorageReadPriority.LOW, "trielog-preload", this::preloadQueue));

      LOG.atInfo()
          .setMessage(
              "Trie log pruning will timeout after {} seconds. If this is timing out, consider using `besu storage trie-log prune` subcommand, see https://besu.hyperledger.org/public-networks/how-to/bonsai-limit-trie-logs")
          .addArgument(timeoutInSeconds)
          .log();

      try {
        future.get(timeoutInSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error loading trie logs from database", e);
        future.cancel(true);
      } catch (TimeoutException e) {
        future.cancel(true);
        LOG.atWarn()
            .setMessage("Timeout occurred while loading and processing {} trie logs from database")
            .addArgument(loadingLimit)
            .log();
      }
    }
    LOG.info("Trie log pruner queue preload complete.");
  }

  private void preloadQueue() {

    try (final Stream<byte[]> trieLogKeys = rootWorldStateStorage.streamTrieLogKeys(loadingLimit)) {

      final AtomicLong addToPruneQueueCount = new AtomicLong();
      final AtomicLong orphansPruned = new AtomicLong();
      trieLogKeys.forEach(
          blockHashAsBytes -> {
            if (Thread.currentThread().isInterrupted()) {
              throw new RuntimeException(
                  new InterruptedException("Thread interrupted during trie log processing."));
            }
            final Hash blockHash = Hash.wrap(Bytes32.wrap(blockHashAsBytes));
            final Optional<BlockHeader> header = blockchain.getBlockHeader(blockHash);
            if (header.isPresent()) {
              addToPruneQueue(header.get().getNumber(), blockHash);
              addToPruneQueueCount.getAndIncrement();
            } else {
              // prune orphaned blocks (sometimes created during block production)
              rootWorldStateStorage.pruneTrieLog(blockHash);
              orphansPruned.getAndIncrement();
              prunedOrphanCounter.inc();
            }
          });

      LOG.atDebug().log("Pruned {} orphaned trie logs from database...", orphansPruned.intValue());
      LOG.atInfo().log(
          "Added {} trie logs to prune queue. Commencing pruning of eligible trie logs...",
          addToPruneQueueCount.intValue());
      int prunedCount = pruneFromQueue();
      LOG.atInfo().log("Pruned {} trie logs", prunedCount);
    } catch (Exception e) {
      if (e instanceof InterruptedException
          || (e.getCause() != null && e.getCause() instanceof InterruptedException)) {
        LOG.info("Operation interrupted, but will attempt to prune what's in the queue so far...");
        int prunedCount = pruneFromQueue();
        LOG.atInfo().log("...pruned {} trie logs", prunedCount);
        Thread.currentThread().interrupt(); // Preserve interrupt status
      } else {
        LOG.error("Error loading trie logs from database, nothing pruned", e);
      }
    }
  }

  public synchronized void addToPruneQueue(final long blockNumber, final Hash blockHash) {
    LOG.atTrace()
        .setMessage("adding trie log to queue for later pruning blockNumber {}; blockHash {}")
        .addArgument(blockNumber)
        .addArgument(blockHash)
        .log();
    trieLogBlocksAndForksByDescendingBlockNumber.put(blockNumber, blockHash);
    addedToPruneQueueCounter.inc();
  }

  public synchronized int pruneFromQueue() {
    final long retainAboveThisBlock = blockchain.getChainHeadBlockNumber() - numBlocksToRetain;
    final Optional<Hash> finalized = blockchain.getFinalized();
    if (requireFinalizedBlock && finalized.isEmpty()) {
      LOG.debug("No finalized block present, skipping pruning");
      return 0;
    }

    final long retainAboveThisBlockOrFinalized =
        finalized
            .flatMap(blockchain::getBlockHeader)
            .map(ProcessableBlockHeader::getNumber)
            .map(finalizedBlock -> Math.min(finalizedBlock, retainAboveThisBlock))
            .orElse(retainAboveThisBlock);

    LOG.atTrace()
        .setMessage(
            "min((chainHeadNumber: {} - numBlocksToRetain: {}) = {}, finalized: {})) = retainAboveThisBlockOrFinalized: {}")
        .addArgument(blockchain::getChainHeadBlockNumber)
        .addArgument(numBlocksToRetain)
        .addArgument(retainAboveThisBlock)
        .addArgument(
            () ->
                finalized
                    .flatMap(blockchain::getBlockHeader)
                    .map(ProcessableBlockHeader::getNumber)
                    .orElse(null))
        .addArgument(retainAboveThisBlockOrFinalized)
        .log();

    final var pruneWindowEntries =
        trieLogBlocksAndForksByDescendingBlockNumber.asMap().entrySet().stream()
            .dropWhile((e) -> e.getKey() > retainAboveThisBlockOrFinalized)
            .limit(pruningLimit);

    final Multimap<Long, Hash> wasPruned = ArrayListMultimap.create();

    pruneWindowEntries.forEach(
        (e) -> {
          for (Hash blockHash : e.getValue()) {
            if (rootWorldStateStorage.pruneTrieLog(blockHash)) {
              wasPruned.put(e.getKey(), blockHash);
            }
          }
        });

    wasPruned.keySet().forEach(trieLogBlocksAndForksByDescendingBlockNumber::removeAll);
    prunedFromQueueCounter.inc(wasPruned.size());

    LOG.atTrace()
        .setMessage("pruned {} trie logs for blocks {}")
        .addArgument(wasPruned::size)
        .addArgument(wasPruned)
        .log();
    LOG.atDebug()
        .setMessage("pruned {} trie logs from {} blocks")
        .addArgument(wasPruned::size)
        .addArgument(() -> wasPruned.keySet().size())
        .log();

    return wasPruned.size();
  }

  @Override
  public void onTrieLogAdded(final TrieLogEvent event) {
    if (TrieLogEvent.Type.ADDED.equals(event.getType())) {
      final Hash blockHash = event.layer().getBlockHash();
      final Optional<Long> blockNumber = event.layer().getBlockNumber();
      blockNumber.ifPresent(
          blockNum ->
              executeAsync.accept(
                  () -> {
                    addToPruneQueue(blockNum, blockHash);
                    pruneFromQueue();
                  }));
    }
  }
}
