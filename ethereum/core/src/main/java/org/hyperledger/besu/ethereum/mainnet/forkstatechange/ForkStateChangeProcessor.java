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
package org.hyperledger.besu.ethereum.mainnet.forkstatechange;

import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

/**
 * Applies a fork's {@link ForkStateChange} at the start of the fork's first block, before the
 * pre-execution system calls and the transactions, and does nothing on any other block. Each {@link
 * org.hyperledger.besu.ethereum.mainnet.ProtocolSpec} provides one, so block import applies it
 * whether the block is processed sequentially or in parallel.
 */
@FunctionalInterface
public interface ForkStateChangeProcessor {

  /** Makes no change, on any block. */
  ForkStateChangeProcessor NONE = context -> {};

  /**
   * Applies the change if the block is the fork's first block.
   *
   * @param context the context of the block being processed
   */
  void process(BlockProcessingContext context);

  /**
   * Returns a processor that applies {@code change} on block {@code forkBlock} only.
   *
   * <p>The change goes through a plain world state updater, so it is not recorded in the block
   * access list or reported to the block tracer. Use it only for forks before block access lists
   * (EIP-7928).
   *
   * @param forkBlock the number of the fork's first block
   * @param change the change to apply
   * @return the processor
   */
  static ForkStateChangeProcessor atBlock(final long forkBlock, final ForkStateChange change) {
    return context -> {
      if (context.getBlockHeader().getNumber() == forkBlock) {
        final WorldUpdater updater = context.getWorldState().updater();
        change.apply(updater);
        updater.commit();
      }
    };
  }
}
