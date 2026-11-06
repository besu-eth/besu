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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogPruner;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A node stopped after persisting the head world state at a block, but before storing that block,
 * restarts with a world state one block ahead of the chain.
 */
public class WorldStateAheadOfChainTest extends AbstractIsolationTests {

  private BonsaiWorldStateKeyValueStorage storage;
  private BlockHeader chainHead;
  private Block unstoredBlock;

  @BeforeEach
  public void persistWorldStateWithoutStoringTheBlock() {
    storage = (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
    chainHead = blockchain.getChainHeadHeader();
    unstoredBlock =
        forTransactions(List.of(burnTransaction(sender1, 0L, Address.fromHexString("0xdeadbeef"))));

    final MutableWorldState headWorldState =
        archive.getWorldState(withBlockHeaderAndUpdateNodeHead(chainHead)).orElseThrow();
    assertThat(processBlock(headWorldState, unstoredBlock).isSuccessful()).isTrue();

    assertThat(storage.getWorldStateBlockHash()).contains(unstoredBlock.getHash());
    assertThat(blockchain.getBlockHeader(unstoredBlock.getHash())).isEmpty();
  }

  @Test
  public void rollsBackToTheChainHeadAfterThePrunerStarts() {
    newTrieLogPruner().initialize();

    assertThat(storage.getTrieLog(unstoredBlock.getHash())).isPresent();
    final Optional<MutableWorldState> chainHeadWorldState =
        restartedArchive().getWorldState(withBlockHeaderAndUpdateNodeHead(chainHead));
    assertThat(chainHeadWorldState).isPresent();
    assertThat(chainHeadWorldState.get().rootHash()).isEqualTo(chainHead.getStateRoot());
    assertThat(storage.getWorldStateBlockHash()).contains(chainHead.getHash());
  }

  @Test
  public void keepsTheWorldStateWhenItsTrieLogIsMissing() {
    storage.pruneTrieLog(unstoredBlock.getHash());

    assertThat(restartedArchive().getWorldState(withBlockHeaderAndUpdateNodeHead(chainHead)))
        .isEmpty();
    assertThat(storage.getWorldStateBlockHash()).contains(unstoredBlock.getHash());
    assertThat(storage.getWorldStateRootHash())
        .contains(unstoredBlock.getHeader().getStateRoot().getBytes());
  }

  private TrieLogPruner newTrieLogPruner() {
    return new TrieLogPruner(
        storage, blockchain, Runnable::run, 512, 100, false, new NoOpMetricsSystem());
  }

  private BonsaiWorldStateProvider restartedArchive() {
    return new BonsaiWorldStateProvider(
        storage,
        blockchain,
        ImmutableExtraStorageConfiguration.builder().maxLayersToLoad(16L).build(),
        new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
        null,
        EvmConfiguration.DEFAULT,
        new BonsaiCodeCache());
  }

  private BlockProcessingResult processBlock(
      final MutableWorldState worldState, final Block block) {
    return protocolSchedule
        .getByBlockHeader(block.getHeader())
        .getBlockProcessor()
        .processBlock(
            BlockExecutionContext.builder()
                .protocolContext(protocolContext)
                .worldState(worldState)
                .block(block)
                .blockAccessList(Optional.empty())
                .build());
  }
}
