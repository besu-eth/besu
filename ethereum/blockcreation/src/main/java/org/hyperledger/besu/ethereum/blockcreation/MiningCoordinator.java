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
package org.hyperledger.besu.ethereum.blockcreation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;

public interface MiningCoordinator {

  void start();

  void stop();

  default void subscribe() {}

  void awaitStop() throws InterruptedException;

  /**
   * If mining is disabled, enable it.
   *
   * @return True if mining is enabled.
   */
  boolean enable();

  /**
   * If mining is enabled, disable it.
   *
   * @return True if mining is disabled.
   */
  boolean disable();

  boolean isMining();

  default void onResumeMining() {}

  default void onPauseMining() {}

  Wei getMinTransactionGasPrice();

  Wei getMinPriorityFeePerGas();

  default void setCoinbase(final Address coinbase) {
    throw new UnsupportedOperationException(
        "Current consensus mechanism prevents setting coinbase.");
  }

  Optional<Address> getCoinbase();

  /**
   * Creates a block if possible, otherwise return an empty result
   *
   * @param parentHeader The parent block's header
   * @param transactions The list of transactions to include
   * @param ommers The list of ommers to include
   * @return If supported, returns the block that was created, otherwise an empty response.
   */
  Optional<Block> createBlock(
      final BlockHeader parentHeader,
      final List<Transaction> transactions,
      final List<BlockHeader> ommers);

  /**
   * Creates a block if possible, otherwise return an empty result
   *
   * @param parentHeader The parent block's header
   * @param timestamp unix timestamp of the new block.
   * @return If supported, returns the block that was created, otherwise an empty response.
   */
  Optional<Block> createBlock(final BlockHeader parentHeader, final long timestamp);

  void changeTargetGasLimit(final Long targetGasLimit);

  default boolean isCompatibleWithEngineApi() {
    return false;
  }
}
