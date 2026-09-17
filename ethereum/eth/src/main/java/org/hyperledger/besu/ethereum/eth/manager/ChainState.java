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
package org.hyperledger.besu.ethereum.eth.manager;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.messages.StatusMessage;
import org.hyperledger.besu.util.Subscribers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

public class ChainState implements ChainHeadEstimate {
  // The best block by total difficulty that we know about
  private final BestBlock bestBlock = new BestBlock();
  // The highest block that we've seen
  private volatile long estimatedHeight = 0L;
  private volatile long earliestBlockHeight = 0L;
  private volatile boolean estimatedHeightKnown = false;

  private final Subscribers<EstimatedHeightListener> estimatedHeightListeners =
      Subscribers.create();

  public long addEstimatedHeightListener(final EstimatedHeightListener listener) {
    return estimatedHeightListeners.subscribe(listener);
  }

  public void removeEstimatedHeightListener(final long listenerId) {
    estimatedHeightListeners.unsubscribe(listenerId);
  }

  public ChainStateSnapshot getSnapshot() {
    return new ChainStateSnapshot(getEstimatedTotalDifficulty(), getEstimatedHeight());
  }

  public boolean hasEstimatedHeight() {
    return estimatedHeightKnown;
  }

  @Override
  public long getEstimatedHeight() {
    return estimatedHeight;
  }

  @Override
  public Difficulty getEstimatedTotalDifficulty() {
    return bestBlock.getTotalDifficulty();
  }

  public BestBlock getBestBlock() {
    return bestBlock;
  }

  public void statusReceived(final StatusMessage statusMessage) {
    synchronized (this) {
      if (statusMessage.isEth69Compatible()) {
        statusReceived(
            statusMessage.bestHash(),
            statusMessage.blockRange().orElseThrow().latestBlock(),
            statusMessage.blockRange().orElseThrow().earliestBlock());
      } else {
        statusReceived(statusMessage.bestHash(), statusMessage.totalDifficulty().orElseThrow());
      }
    }
  }

  @VisibleForTesting
  public void statusReceived(final Hash bestBlockHash, final Difficulty bestBlockTotalDifficulty) {
    synchronized (this) {
      bestBlock.totalDifficulty = bestBlockTotalDifficulty;
      bestBlock.hash = bestBlockHash;
    }
  }

  private void statusReceived(
      final Hash bestBlockHash, final long bestBlockNumber, final long earliestBlockHeight) {
    synchronized (this) {
      this.bestBlock.hash = bestBlockHash;
      this.bestBlock.number = bestBlockNumber;
      this.earliestBlockHeight = earliestBlockHeight;
      updateHeightEstimate(bestBlockNumber);
    }
  }

  public void update(final Hash blockHash, final long blockNumber) {
    synchronized (this) {
      if (bestBlock.hash.equals(blockHash)) {
        bestBlock.number = blockNumber;
      }
      updateHeightEstimate(blockNumber);
    }
  }

  public void update(final Hash blockHash, final long blockNumber, final long earliestBlockHeight) {
    synchronized (this) {
      if (bestBlock.hash.equals(blockHash)) {
        bestBlock.number = blockNumber;
      }
      updateHeightEstimate(blockNumber);
      this.earliestBlockHeight = earliestBlockHeight;
    }
  }

  public void update(final BlockHeader header) {
    synchronized (this) {
      if (header.getHash().equals(bestBlock.hash)) {
        bestBlock.number = header.getNumber();
      }
      updateHeightEstimate(header.getNumber());
    }
  }

  public void updateForAnnouncedBlock(
      final BlockHeader blockHeader, final Difficulty totalDifficulty) {
    synchronized (this) {
      // Blocks are announced before they're imported so their chain head must be the parent
      final Difficulty parentTotalDifficulty =
          totalDifficulty.subtract(blockHeader.getDifficulty());
      final long parentBlockNumber = blockHeader.getNumber() - 1;
      if (parentTotalDifficulty.compareTo(bestBlock.totalDifficulty) >= 0) {
        bestBlock.totalDifficulty = parentTotalDifficulty;
        bestBlock.hash = blockHeader.getParentHash();
        bestBlock.number = parentBlockNumber;
      }
      updateHeightEstimate(parentBlockNumber);
    }
  }

  public void updateHeightEstimate(final long blockNumber) {
    synchronized (this) {
      if (blockNumber > estimatedHeight) {
        estimatedHeightKnown = true;
        estimatedHeight = blockNumber;
        estimatedHeightListeners.forEach(e -> e.onEstimatedHeightChanged(estimatedHeight));
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("earliestBlockHeight", earliestBlockHeight)
        .add("estimatedHeight", estimatedHeight)
        .add("bestBlock", bestBlock)
        .toString();
  }

  // Represent the best block by totalDifficulty
  public static class BestBlock {
    volatile long number = 0L;
    volatile Hash hash = null;
    volatile Difficulty totalDifficulty = Difficulty.ZERO;

    public long getNumber() {
      return number;
    }

    public Hash getHash() {
      return hash;
    }

    public Difficulty getTotalDifficulty() {
      return totalDifficulty;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("totalDifficulty", totalDifficulty)
          .add("blockHash", hash)
          .add("number", number)
          .toString();
    }
  }

  @FunctionalInterface
  public interface EstimatedHeightListener {
    void onEstimatedHeightChanged(long estimatedHeight);
  }
}
