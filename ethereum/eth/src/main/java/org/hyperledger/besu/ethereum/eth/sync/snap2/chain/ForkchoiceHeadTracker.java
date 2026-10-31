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
package org.hyperledger.besu.ethereum.eth.sync.snap2.chain;

import org.hyperledger.besu.consensus.merge.ForkchoiceEvent;
import org.hyperledger.besu.consensus.merge.NewPayloadListener;
import org.hyperledger.besu.consensus.merge.UnverifiedForkchoiceListener;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.eth.sync.PivotBlockSelector;
import org.hyperledger.besu.ethereum.eth.sync.snapsync.SnapSyncProcessState;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.annotations.VisibleForTesting;

/**
 * Follows the head the consensus layer sends with forkchoice updates. Headers received with new
 * payloads are cached: their hash commits to their content, so once the consensus layer names a
 * hash, the matching header is trusted.
 */
public final class ForkchoiceHeadTracker
    implements ChainHeadSource,
        UnverifiedForkchoiceListener,
        NewPayloadListener,
        PivotBlockSelector {

  @VisibleForTesting static final int CACHED_HEADERS = 512;

  private final Runnable cleanup;
  private final Map<Hash, BlockHeader> headers =
      new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(final Map.Entry<Hash, BlockHeader> eldest) {
          return size() > CACHED_HEADERS;
        }
      };
  private volatile Hash head;

  public ForkchoiceHeadTracker(final Runnable cleanup) {
    this.cleanup = Objects.requireNonNull(cleanup);
  }

  @Override
  public void onNewUnverifiedForkchoice(final ForkchoiceEvent event) {
    if (event.getHeadBlockHash() != null && !event.getHeadBlockHash().equals(Hash.ZERO)) {
      head = event.getHeadBlockHash();
    }
  }

  @Override
  public void onNewPayload(final BlockHeader header) {
    synchronized (headers) {
      headers.put(header.getHash(), header);
    }
  }

  @Override
  public Optional<Hash> headHash() {
    return Optional.ofNullable(head);
  }

  @Override
  public Optional<BlockHeader> knownHeader(final Hash hash) {
    synchronized (headers) {
      return Optional.ofNullable(headers.get(hash));
    }
  }

  @Override
  public CompletableFuture<SnapSyncProcessState> selectNewPivotBlock() {
    return CompletableFuture.failedFuture(
        new UnsupportedOperationException("the SNAP2 sync selects its own pivot"));
  }

  @Override
  public CompletableFuture<Void> prepareRetry() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public long getBestChainHeight() {
    return headHash().flatMap(this::knownHeader).map(BlockHeader::getNumber).orElse(0L);
  }

  @Override
  public void close() {
    cleanup.run();
  }
}
