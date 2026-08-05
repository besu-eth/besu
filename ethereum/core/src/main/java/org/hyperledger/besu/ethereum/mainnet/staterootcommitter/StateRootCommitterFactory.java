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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockMetrics;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.worldstate.StateRootCommitter;

import java.util.Optional;

public interface StateRootCommitterFactory {
  StateRootCommitter forBlock(
      ProtocolContext protocolContext, BlockHeader blockHeader, Optional<BlockAccessList> maybeBal);

  /**
   * Creates the committer for a block, letting a BAL-backed implementation report how long its
   * background state-root computation took.
   *
   * @param protocolContext the protocol context
   * @param blockHeader the header of the block being processed
   * @param maybeBal the block's access list, when it has one
   * @param slowBlockMetrics the per-block slow-block aggregate, or null when tracing is disabled
   * @return the state root committer for this block
   */
  default StateRootCommitter forBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBal,
      final SlowBlockMetrics slowBlockMetrics) {
    return forBlock(protocolContext, blockHeader, maybeBal);
  }
}
