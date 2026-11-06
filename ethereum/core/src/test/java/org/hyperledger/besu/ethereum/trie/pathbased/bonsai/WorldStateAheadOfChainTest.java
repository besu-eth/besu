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
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead;
import static org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.provider.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogPruner;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A node stopped after persisting the head world state at a block, but before storing that block,
 * restarts with the world state one block ahead of the chain head.
 */
public class WorldStateAheadOfChainTest extends AbstractIsolationTests {

  private final Address recipient = Address.fromHexString("0xdeadbeef");
  private BonsaiWorldStateKeyValueStorage storage;
  private BlockHeader genesisHeader;
  private BlockHeader chainHead;
  private Block unstoredBlock;

  @BeforeEach
  public void persistWorldStateWithoutStoringTheBlock() {
    storage = (BonsaiWorldStateKeyValueStorage) worldStateKeyValueStorage;
    genesisHeader = blockchain.getChainHeadHeader();
    final Block storedBlock = forTransactions(List.of(burnTransaction(sender1, 0L, recipient)));
    assertThat(executeBlock(archive.getWorldState(), storedBlock).isSuccessful()).isTrue();
    chainHead = storedBlock.getHeader();

    unstoredBlock = forTransactions(List.of(burnTransaction(sender1, 1L, recipient)));
    final MutableWorldState headWorldState =
        archive.getWorldState(withBlockHeaderAndUpdateNodeHead(chainHead)).orElseThrow();
    assertThat(processBlock(headWorldState, unstoredBlock).isSuccessful()).isTrue();

    assertThat(storage.getWorldStateBlockHash()).contains(unstoredBlock.getHash());
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(chainHead);
  }

  @Test
  public void restartRollsTheWorldStateBackToTheChainHead() {
    final BonsaiWorldStateProvider restarted = createArchive();

    assertThat(storage.getWorldStateBlockHash()).contains(chainHead.getHash());
    assertThat(restarted.getWorldState().rootHash()).isEqualTo(chainHead.getStateRoot());
    assertThat(restarted.getWorldState(withBlockHeaderAndNoUpdateNodeHead(chainHead))).isPresent();
  }

  @Test
  public void restartedNodeImportsTheUnstoredBlock() {
    final BonsaiWorldStateProvider restarted = createArchive();
    new TrieLogPruner(storage, blockchain, Runnable::run, 512, 100, false, new NoOpMetricsSystem())
        .initialize();

    final MutableWorldState chainHeadWorldState =
        restarted.getWorldState(withBlockHeaderAndUpdateNodeHead(chainHead)).orElseThrow();
    assertThat(executeBlock(chainHeadWorldState, unstoredBlock).isSuccessful()).isTrue();
    assertThat(storage.getWorldStateBlockHash()).contains(unstoredBlock.getHash());
    assertThat(blockchain.getChainHeadHeader()).isEqualTo(unstoredBlock.getHeader());
  }

  @Test
  public void restartedNodeRollsBackBelowTheChainHead() {
    final Optional<MutableWorldState> genesisWorldState =
        createArchive().getWorldState(withBlockHeaderAndUpdateNodeHead(genesisHeader));

    assertThat(genesisWorldState).isPresent();
    assertThat(genesisWorldState.get().rootHash()).isEqualTo(genesisHeader.getStateRoot());
  }
}
