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
package org.hyperledger.besu.ethereum.eth.sync.backwardsync;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.List;

public interface BadChainListener {
  /**
   * Called when a block of the backward chain is known to be bad, with the descendants the chain
   * holds for it that are not marked as bad yet.
   *
   * @param badBlock the header of the bad block, its body is not always known
   * @param badBlockDescendants descendants whose body is known, ordered towards the head
   * @param badBlockHeaderDescendants descendants only known by header, ordered towards the head
   */
  void onBadChain(
      final BlockHeader badBlock,
      final List<Block> badBlockDescendants,
      final List<BlockHeader> badBlockHeaderDescendants);
}
