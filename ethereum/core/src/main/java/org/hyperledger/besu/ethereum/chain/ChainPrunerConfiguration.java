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
package org.hyperledger.besu.ethereum.chain;

import org.hyperledger.besu.ethereum.chain.ChainDataPruner.ChainPruningStrategy;

public record ChainPrunerConfiguration(
    ChainPruningStrategy pruningMode,
    long chainPruningBlocksRetained,
    long chainPruningBalsRetained,
    long chainPruningRetainedMinimum,
    long chainPruningFrequency,
    int preMergePruningBlocksQuantity) {

  /**
   * The history expiry window of EIP-4444, matching the consensus layer block retention window.
   * EIP-7928 requires block access lists to stay available for at least this many epochs.
   */
  public static final long HISTORY_PRUNE_EPOCHS = 14299L;

  /** The number of slots per epoch on the consensus layer. */
  public static final long SLOTS_PER_EPOCH = 32L;

  /** The history expiry window converted to blocks. */
  public static final long HISTORY_PRUNE_BLOCKS = HISTORY_PRUNE_EPOCHS * SLOTS_PER_EPOCH;

  public static final ChainPrunerConfiguration DEFAULT =
      new ChainPrunerConfiguration(
          ChainPruningStrategy.NONE,
          HISTORY_PRUNE_BLOCKS,
          HISTORY_PRUNE_BLOCKS,
          HISTORY_PRUNE_BLOCKS,
          256,
          1000);

  /**
   * Check if block chain pruning is enabled.
   *
   * @return true if ALL mode is enabled
   */
  public boolean isBlockPruningEnabled() {
    return pruningMode == ChainPruningStrategy.ALL;
  }

  /**
   * Check if BAL pruning is enabled.
   *
   * @return true if BAL or ALL mode is enabled
   */
  public boolean isBalPruningEnabled() {
    return pruningMode == ChainPruningStrategy.BAL || pruningMode == ChainPruningStrategy.ALL;
  }
}
