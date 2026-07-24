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
package org.hyperledger.besu.ethereum.mainnet.staterootcommitter;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.trie.forest.ForestWorldStateArchive;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.PathBasedWorldStateProvider;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Optional;

/**
 * Picks one committer per block:
 *
 * <ul>
 *   <li>{@link ForestStateRootCommitter} — Forest archive
 *   <li>{@link BalStateRootCommitter} — Bonsai + BAL background root
 *   <li>{@link DefaultStateRootCommitter} — Bonsai accumulator at persist
 * </ul>
 */
public final class StateRootCommitterFactory {

  private enum Mode {
    BAL,
    DEFAULT,
    FOREST
  }

  private final BalConfiguration balConfiguration;

  public StateRootCommitterFactory(final BalConfiguration balConfiguration) {
    this.balConfiguration = balConfiguration;
  }

  public StateRootCommitter forBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> maybeBal,
      final boolean storageFrozen) {
    return switch (resolveMode(protocolContext, maybeBal)) {
      case BAL ->
          new BalStateRootCommitter(
              protocolContext,
              blockHeader,
              BlockAccessListAddressView.of(maybeBal.get()),
              storageFrozen);
      case DEFAULT -> new DefaultStateRootCommitter();
      case FOREST -> ForestStateRootCommitter.INSTANCE;
    };
  }

  private Mode resolveMode(
      final ProtocolContext protocolContext, final Optional<BlockAccessList> maybeBal) {
    if (protocolContext.getWorldStateArchive() instanceof ForestWorldStateArchive) {
      return Mode.FOREST;
    }
    if (maybeBal.isPresent()
        && balConfiguration.isBalStateRootEnabled()
        && !isTrieDisabled(protocolContext)) {
      return Mode.BAL;
    }
    return Mode.DEFAULT;
  }

  private static boolean isTrieDisabled(final ProtocolContext protocolContext) {
    return protocolContext.getWorldStateArchive() instanceof PathBasedWorldStateProvider provider
        && provider.getWorldStateSharedSpec().isTrieDisabled();
  }
}
