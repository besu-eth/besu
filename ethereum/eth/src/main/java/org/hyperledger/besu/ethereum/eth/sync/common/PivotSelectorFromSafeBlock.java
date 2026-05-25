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
package org.hyperledger.besu.ethereum.eth.sync.common;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceListener;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the pivot block for snap sync from the {@link NewPayloadHeaderCache}.
 *
 * <p>The cache is populated by {@code engine_newPayload} events; on every {@code
 * engine_forkchoiceUpdated} the selector additionally ensures the head and safe block headers are
 * cached (downloading from a peer if necessary). {@link #selectNewPivotBlock()} then picks a pivot
 * purely from the cache, in this order:
 *
 * <ol>
 *   <li>the FCU's safe block, if it is fewer than {@value #SAFE_BLOCK_FRESHNESS_THRESHOLD} blocks
 *       behind the head;
 *   <li>the cached header at {@code head - }{@value #PIVOT_OFFSET_FROM_HEAD};
 *   <li>the oldest cached header if fewer than {@value #PIVOT_OFFSET_FROM_HEAD} are cached;
 *   <li>a failed future if none of the above is available.
 * </ol>
 */
public class PivotSelectorFromSafeBlock
    implements PivotBlockSelector, UnverifiedForkchoiceListener {

  private static final Logger LOG = LoggerFactory.getLogger(PivotSelectorFromSafeBlock.class);
  private static final long DIAGNOSTIC_LOG_RATE_LIMIT = Duration.ofMinutes(1).toMillis();

  /** Offset from head to use as pivot when the safe block is too far behind (1 epoch of slots). */
  public static final int PIVOT_OFFSET_FROM_HEAD = 32;

  /** Maximum distance (in blocks) the safe block can be behind head and still be used as pivot. */
  public static final int SAFE_BLOCK_FRESHNESS_THRESHOLD =
      NewPayloadHeaderCache.MAX_SIZE - PIVOT_OFFSET_FROM_HEAD;

  private final ProtocolContext protocolContext;
  private final GenesisConfigOptions genesisConfig;
  private final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier;
  private final SingleBlockHeaderDownloader headerDownloader;
  private final NewPayloadHeaderCache headerCache;
  private final Runnable cleanupAction;

  private volatile long lastNoFcuInfoLog = System.currentTimeMillis();

  /**
   * Construct a pivot selector. The caller is responsible for registering {@code headerCache} as a
   * {@code NewPayloadListener} and this selector as an {@code UnverifiedForkchoiceListener} on the
   * merge context, and for unsubscribing both via {@code cleanupAction}.
   */
  public PivotSelectorFromSafeBlock(
      final ProtocolContext protocolContext,
      final GenesisConfigOptions genesisConfig,
      final Supplier<Optional<ForkchoiceEvent>> forkchoiceStateSupplier,
      final SingleBlockHeaderDownloader headerDownloader,
      final NewPayloadHeaderCache headerCache,
      final Runnable cleanupAction) {
    this.protocolContext = protocolContext;
    this.genesisConfig = genesisConfig;
    this.forkchoiceStateSupplier = forkchoiceStateSupplier;
    this.headerDownloader = headerDownloader;
    this.headerCache = headerCache;
    this.cleanupAction = cleanupAction;
  }

  /**
   * On every FCU, make sure the safe and head block headers are cached. If either is missing we
   * fire off a peer download and feed the result into the cache.
   */
  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    if (event.hasValidSafeBlockHash()) {
      ensureCached(event.getSafeBlockHash());
    }
    ensureCached(event.getHeadBlockHash());
  }

  private void ensureCached(final Hash hash) {
    if (Hash.ZERO.equals(hash) || headerCache.getByHash(hash).isPresent()) {
      return;
    }
    headerDownloader
        .downloadBlockHeader(hash)
        .thenAccept(headerCache::put)
        .exceptionally(
            t -> {
              LOG.debug("Failed to download header {} for pivot cache: {}", hash, t.toString());
              return null;
            });
  }

  @Override
  public CompletableFuture<PivotSyncState> selectNewPivotBlock() {
    final Optional<ForkchoiceEvent> maybeForkchoice = forkchoiceStateSupplier.get();
    if (maybeForkchoice.isEmpty()) {
      return logAndFailNoFcu();
    }
    final ForkchoiceEvent fcu = maybeForkchoice.get();

    final Optional<BlockHeader> maybeHead = headerCache.getByHash(fcu.getHeadBlockHash());
    if (maybeHead.isEmpty()) {
      // The FCU listener will start a download asynchronously; nothing to do this cycle.
      return CompletableFuture.failedFuture(
          new RuntimeException("Head block header not yet cached"));
    }
    final BlockHeader head = maybeHead.get();

    if (fcu.hasValidSafeBlockHash()) {
      final Optional<BlockHeader> maybeSafe = headerCache.getByHash(fcu.getSafeBlockHash());
      if (maybeSafe.isPresent()
          && head.getNumber() - maybeSafe.get().getNumber() < SAFE_BLOCK_FRESHNESS_THRESHOLD) {
        LOG.debug("Returning safe block {} as pivot", maybeSafe.get().getNumber());
        return CompletableFuture.completedFuture(new PivotSyncState(maybeSafe.get(), true));
      }
    }

    // Safe block is missing or too far behind head. Use a cached header `PIVOT_OFFSET_FROM_HEAD`
    // behind head (≈ one epoch — far enough to be settled, close enough to be served).
    final Optional<BlockHeader> maybeOffset =
        headerCache.getByNumber(head.getNumber() - PIVOT_OFFSET_FROM_HEAD);
    if (maybeOffset.isPresent()) {
      LOG.warn(
          "Safe block too far from head — using head-{} ({}) from newPayload cache as pivot.",
          PIVOT_OFFSET_FROM_HEAD,
          maybeOffset.get().getNumber());
      return CompletableFuture.completedFuture(new PivotSyncState(maybeOffset.get(), true));
    }

    // Cache is too small to offer a head-N entry. If the cache is "young" (<32 entries) return
    // its oldest header as a best-effort pivot; otherwise treat as unable to select.
    if (headerCache.size() < PIVOT_OFFSET_FROM_HEAD) {
      final Optional<BlockHeader> oldest = headerCache.oldest();
      if (oldest.isPresent()) {
        LOG.warn(
            "newPayload cache too small ({} entries) — falling back to oldest cached header ({}) as pivot.",
            headerCache.size(),
            oldest.get().getNumber());
        return CompletableFuture.completedFuture(new PivotSyncState(oldest.get(), true));
      }
    }

    return CompletableFuture.failedFuture(
        new RuntimeException("No suitable pivot block available"));
  }

  private CompletableFuture<PivotSyncState> logAndFailNoFcu() {
    final long now = System.currentTimeMillis();
    if (lastNoFcuInfoLog + DIAGNOSTIC_LOG_RATE_LIMIT < now) {
      lastNoFcuInfoLog = now;
      LOG.info(
          "Waiting for consensus client, this may be because your consensus client is still syncing");
    }
    LOG.debug("No forkchoice update received yet");
    return CompletableFuture.failedFuture(
        new RuntimeException("No forkchoice update received yet"));
  }

  @Override
  public CompletableFuture<Void> prepareRetry() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    cleanupAction.run();
  }

  @Override
  public long getMinRequiredBlockNumber() {
    return genesisConfig.getTerminalBlockNumber().orElse(0L);
  }

  @Override
  public long getBestChainHeight() {
    final long localChainHeight = protocolContext.getBlockchain().getChainHeadBlockNumber();
    final long cachedHeadNumber =
        forkchoiceStateSupplier
            .get()
            .map(ForkchoiceEvent::getHeadBlockHash)
            .flatMap(headerCache::getByHash)
            .map(BlockHeader::getNumber)
            .orElse(0L);
    return Math.max(cachedHeadNumber, localChainHeight);
  }
}
