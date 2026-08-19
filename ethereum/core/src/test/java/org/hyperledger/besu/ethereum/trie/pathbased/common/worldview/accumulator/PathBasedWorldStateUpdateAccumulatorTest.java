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
package org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.BonsaiWorldStateUpdateAccumulator;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

/** Tests for {@link PathBasedWorldStateUpdateAccumulator#importStateChangesFromPartialView}. */
class PathBasedWorldStateUpdateAccumulatorTest {

  private static final Address ACCOUNT =
      Address.fromHexString("0x1000000000000000000000000000000000000001");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));
  private static final UInt256 V0 = UInt256.valueOf(100);
  private static final UInt256 V1 = UInt256.valueOf(200);
  private static final UInt256 V2 = UInt256.valueOf(300);

  @Test
  void importPartialView_sameSlotTwoTransactions_keepsBlockStartPriorAndFinalUpdated() {
    try (BonsaiWorldState worldState = newEmptyWorldState()) {
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();
      accumulator.createAccount(ACCOUNT, 0L, Wei.ONE);
      accumulator.getAccount(ACCOUNT).setStorageValue(SLOT.getSlotKey().orElseThrow(), V0);
      accumulator.commit();
      worldState.persist(null);

      final PartialBlockAccessView tx0 = partialView(0, V0, V1);
      final PartialBlockAccessView tx1 = partialView(1, V1, V2);

      accumulator.importStateChangesFromPartialView(tx0);
      accumulator.importStateChangesFromPartialView(tx1);

      final PathBasedValue<UInt256> merged =
          accumulator.getStorageToUpdate().get(ACCOUNT).get(SLOT);
      assertThat(merged.getPrior()).isEqualTo(V0);
      assertThat(merged.getUpdated()).isEqualTo(V2);

      final TrieLogLayer trieLog =
          new BonsaiTrieLogFactory()
              .create(
                  accumulator,
                  new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
      final PathBasedValue<UInt256> trieLogSlot = trieLog.getStorageChanges(ACCOUNT).get(SLOT);
      assertThat(trieLogSlot.getPrior()).isEqualTo(V0);
      assertThat(trieLogSlot.getUpdated()).isEqualTo(V2);
    }
  }

  private static PartialBlockAccessView partialView(
      final long txIndex, final UInt256 prior, final UInt256 updated) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder.getOrCreateAccountBuilder(ACCOUNT).addStorageChange(SLOT, prior, updated);
    return builder.build();
  }

  private static BonsaiWorldState newEmptyWorldState() {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
    return new BonsaiWorldState(
        storage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiWorldStateCacheManager(
            storage, EvmConfiguration.DEFAULT, new PathBasedCodeCache()),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        new PathBasedCodeCache());
  }
}
