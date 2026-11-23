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
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Last stage of the headers pipeline: checks that each batch, highest header first, is the parent
 * chain of the headers already stored, then stores it. It also finds the finalized block the
 * consensus client sent, which makes every header below it finalized too.
 */
class StoreHeadersStep implements Consumer<List<BlockHeader>> {

  private static final Logger LOG = LoggerFactory.getLogger(StoreHeadersStep.class);
  private static final long LOG_INTERVAL_MILLIS = 10_000;

  private final HeaderStore headerStore;
  private final Supplier<Hash> finalizedBlockHash;
  private final long targetNumber;
  private final long headersToStore;
  private BlockHeader lowest;
  private long finalizedNumber;
  private long lastLogMillis;

  /**
   * @param target the header of the head to sync to, already stored
   * @param anchorNumber the block the stored headers must reach down to, excluded
   */
  StoreHeadersStep(
      final HeaderStore headerStore,
      final BlockHeader target,
      final long anchorNumber,
      final Supplier<Hash> finalizedBlockHash) {
    this.headerStore = headerStore;
    this.finalizedBlockHash = finalizedBlockHash;
    this.targetNumber = target.getNumber();
    this.headersToStore = target.getNumber() - anchorNumber;
    this.lowest = target;
    this.finalizedNumber =
        target.getHash().equals(finalizedBlockHash.get()) ? target.getNumber() : -1;
  }

  @Override
  public void accept(final List<BlockHeader> headers) {
    final Hash finalized = finalizedBlockHash.get();
    for (final BlockHeader header : headers) {
      if (!header.getHash().equals(lowest.getParentHash())) {
        throw new IllegalStateException(
            "Header #" + header.getNumber() + " is not the parent of " + lowest.toLogString());
      }
      if (header.getHash().equals(finalized)) {
        finalizedNumber = header.getNumber();
      }
      lowest = header;
    }
    headerStore.putAll(headers);
    logProgress();
  }

  /** The lowest stored header, the first one to import if its parent is the anchor. */
  BlockHeader lowest() {
    return lowest;
  }

  /** The number of the finalized block among the stored headers, -1 if it is not one of them. */
  long finalizedNumber() {
    return finalizedNumber;
  }

  private void logProgress() {
    final long now = System.currentTimeMillis();
    if (now - lastLogMillis >= LOG_INTERVAL_MILLIS) {
      lastLogMillis = now;
      final long stored = targetNumber - lowest.getNumber() + 1;
      LOG.info(
          String.format(
              "Backward sync downloaded %,d of %,d headers (%.1f%%)",
              stored, headersToStore, 100.0 * stored / headersToStore));
    }
  }
}
