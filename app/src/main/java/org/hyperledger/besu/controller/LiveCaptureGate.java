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
package org.hyperledger.besu.controller;

import org.hyperledger.besu.ethereum.eth.sync.state.SyncState;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Computes the highest block number that is safe to commit to the trie-node history archive.
 *
 * <p>When the network head is not reliably known — e.g. during PoS backward sync where the CL
 * only sends {@code forkchoiceUpdated} (not {@code newPayload}) and PoS peers do not advertise
 * height estimates — {@link Long#MAX_VALUE} is returned so that every block being imported is
 * captured. This covers the common case where a PoS archive node is catching up from genesis
 * with {@code sync-mode=FULL}.
 *
 * <p>Once the network head is known ({@link SyncState#isNetworkHeadKnown()} returns {@code true}),
 * the gate trails the live network head by {@code maxLayers} so that blocks within the reorg
 * window are not archived prematurely.
 */
class LiveCaptureGate implements LongSupplier {

  private final SyncState syncState;
  private final long maxLayers;
  private final AtomicLong highestSafeBlock = new AtomicLong(Long.MIN_VALUE);

  LiveCaptureGate(final SyncState syncState, final long maxLayers) {
    this.syncState = syncState;
    this.maxLayers = maxLayers;
  }

  @Override
  public long getAsLong() {
    if (!syncState.isNetworkHeadKnown()) {
      return Long.MAX_VALUE;
    }
    return highestSafeBlock.accumulateAndGet(
        syncState.bestChainHeight() - maxLayers, Math::max);
  }
}
