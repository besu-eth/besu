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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_HASH_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.proof.WorldStateProof;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

public class BonsaiArchiveProofsIntegrationTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Wei BALANCE = Wei.of(42L);
  // interval=100 → nearestCheckpoint for block 99 = ((99+100)/100)*100 - 1 = 99
  // so block 99 is its own checkpoint (no trie-log rollback needed)
  private static final long INTERVAL = 100L;
  private static final long TARGET_BLOCK_NUMBER = INTERVAL - 1; // 99

  @Test
  void stateProofsEnabled_getAccountProof_returnsValidProof() {
    final var config =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(512L)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .stateProofsEnabled(true)
                            .archiveTrieNodeCheckpointInterval(INTERVAL)
                            .build())
                    .build())
            .build();

    final BonsaiWorldStateKeyValueStorage mainStorage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(), new NoOpMetricsSystem(), config);

    // Minimal blockchain mock — provider constructor needs getChainHeadHeader()
    final Blockchain blockchain = mock(Blockchain.class);
    final BlockHeader placeholder = new BlockHeaderTestFixture().number(0L).buildHeader();
    when(blockchain.getChainHeadHeader()).thenReturn(placeholder);

    final BonsaiArchiveWorldStateProvider provider =
        new BonsaiArchiveWorldStateProvider(
            mainStorage,
            blockchain,
            config,
            new BonsaiCachedMerkleTrieLoader(new NoOpMetricsSystem()),
            null,
            EvmConfiguration.DEFAULT,
            () -> null,
            new CodeCache(),
            new NoOpMetricsSystem());

    // Seed state: create one account and persist — writes trie nodes to
    // TRIE_BRANCH_STORAGE_ARCHIVE at suffix=0 (genesis window, interval=100).
    // Use persist(null) to skip state-root header verification.
    final BonsaiWorldState seedState =
        new BonsaiWorldState(
            provider,
            mainStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
    final WorldUpdater updater = seedState.updater();
    updater.createAccount(ADDRESS, 0, BALANCE);
    updater.commit();
    seedState.persist(null);
    final Hash actualStateRoot = seedState.rootHash();

    // Block 99: nearestCheckpoint = ((99+100)/100)*100 - 1 = 99, so block 99 IS its own
    // checkpoint — resetWorldStateToCheckpoint returns immediately, no trie-log rollback.
    final BlockHeader targetHeader =
        new BlockHeaderTestFixture()
            .number(TARGET_BLOCK_NUMBER)
            .stateRoot(actualStateRoot)
            .buildHeader();

    // getCheckpointStateStartBlock: getBlockHeader(targetHash) → targetHeader (number=99),
    // nearestCheckpoint=99, getBlockHeaderSafe(99) → targetHeader (returned directly).
    when(blockchain.getBlockHeader(targetHeader.getHash())).thenReturn(Optional.of(targetHeader));
    when(blockchain.getBlockHeaderSafe(TARGET_BLOCK_NUMBER)).thenReturn(Optional.of(targetHeader));

    // Point WORLD_BLOCK_HASH_KEY at targetHeader so the head world-state lookup works.
    final var tx = mainStorage.getComposedWorldStateStorage().startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_HASH_KEY,
        targetHeader.getHash().getBytes().toArrayUnsafe());
    tx.commit();

    // getAccountProof enters the proofs branch (stateProofsEnabled=true,
    // !shouldWorldStateUpdateHead=true), creates a proofWorldState backed by
    // archiveReadStorage (archiveReadsEnabled=true), resets to the checkpoint (block 99
    // = target → no rollback), then traverses the archived MPT from actualStateRoot.
    // Trie nodes written to TRIE_BRANCH_STORAGE_ARCHIVE at suffix=0 during persist(null)
    // are found via nearest-before lookup at block-number suffix=99.
    final Optional<WorldStateProof> proof =
        provider.getAccountProof(targetHeader, ADDRESS, List.of(), Function.identity());

    assertThat(proof).isPresent();
    assertThat(proof.get().getAccountProof()).isNotEmpty();
    assertThat(proof.get().getStateTrieAccountValue())
        .isPresent()
        .hasValueSatisfying(v -> assertThat(v.getBalance()).isEqualByComparingTo(BALANCE));
  }
}
