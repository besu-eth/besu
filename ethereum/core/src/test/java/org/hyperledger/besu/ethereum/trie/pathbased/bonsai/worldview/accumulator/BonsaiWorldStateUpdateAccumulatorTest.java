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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.WorldStateConfig.createStatefulConfigWithTrie;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrieException;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.BonsaiTrieLogFactory;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.NoOpTrieLogManager;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogLayer;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.NoOpBonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.cache.NoOpBonsaiWorldStateCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutableExtraStorageConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link BonsaiWorldStateUpdateAccumulator}. */
class BonsaiWorldStateUpdateAccumulatorTest {

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

      final BonsaiValue<UInt256> merged = accumulator.getStorageToUpdate().get(ACCOUNT).get(SLOT);
      assertThat(merged.getPrior()).isEqualTo(V0);
      assertThat(merged.getUpdated()).isEqualTo(V2);

      final TrieLogLayer trieLog =
          new BonsaiTrieLogFactory()
              .create(
                  accumulator,
                  new BlockHeaderTestFixture().number(1).stateRoot(Hash.EMPTY).buildHeader());
      final BonsaiValue<UInt256> trieLogSlot = trieLog.getStorageChanges(ACCOUNT).get(SLOT);
      assertThat(trieLogSlot.getPrior()).isEqualTo(V0);
      assertThat(trieLogSlot.getUpdated()).isEqualTo(V2);
    }
  }

  @ParameterizedTest(name = "codeStoredByCodeHash={0}")
  @ValueSource(booleans = {true, false})
  void unreadableCode_throwsAndIsNotRecorded_soARetryReadsItAgain(
      final boolean codeStoredByCodeHash) {
    final Bytes code = Bytes.fromHexString("0x5b00");
    final Hash codeHash = Hash.hash(code);
    final Bytes otherCode = Bytes.fromHexString("0x00");
    final BonsaiCodeCache codeCache = new BonsaiCodeCache();
    try (BonsaiWorldState worldState =
        newEmptyWorldState(codeStorageConfig(codeStoredByCodeHash), codeCache)) {
      final BonsaiWorldStateKeyValueStorage storage = worldState.getWorldStateStorage();
      // only other code is stored for the account, so its own code cannot be read
      final BonsaiWorldStateKeyValueStorage.Updater accountUpdater = storage.updater();
      accountUpdater.putAccountInfoState(
          ACCOUNT.addressHash(),
          RLP.encode(
              new PmtStateTrieAccountValue(0L, Wei.ONE, Hash.EMPTY_TRIE_HASH, codeHash)::writeTo));
      accountUpdater.putCode(ACCOUNT.addressHash(), Hash.hash(otherCode), otherCode);
      accountUpdater.commit();
      final BonsaiWorldStateUpdateAccumulator accumulator =
          (BonsaiWorldStateUpdateAccumulator) worldState.updater();

      assertThatThrownBy(() -> accumulator.get(ACCOUNT).getCode())
          .isInstanceOf(MerkleTrieException.class);
      assertThat(accumulator.getCodeToUpdate()).doesNotContainKey(ACCOUNT);
      assertThat(codeCache.getIfPresent(codeHash)).isNull();

      final BonsaiWorldStateKeyValueStorage.Updater codeUpdater = storage.updater();
      codeUpdater.putCode(ACCOUNT.addressHash(), codeHash, code);
      codeUpdater.commit();
      assertThat(accumulator.get(ACCOUNT).getCode()).isEqualTo(code);
    }
  }

  private static PartialBlockAccessView partialView(
      final long txIndex, final UInt256 prior, final UInt256 updated) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(txIndex);
    builder.getOrCreateAccountBuilder(ACCOUNT).addStorageChange(SLOT, prior, updated);
    return builder.build();
  }

  private static DataStorageConfiguration codeStorageConfig(final boolean codeStoredByCodeHash) {
    return ImmutableDataStorageConfiguration.builder()
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .extraStorageConfiguration(
            ImmutableExtraStorageConfiguration.builder()
                .unstable(
                    ImmutableExtraStorageConfiguration.Unstable.builder()
                        .codeStoredByCodeHashEnabled(codeStoredByCodeHash)
                        .build())
                .build())
        .build();
  }

  private static BonsaiWorldState newEmptyWorldState() {
    return newEmptyWorldState(
        DataStorageConfiguration.DEFAULT_BONSAI_CONFIG, new BonsaiCodeCache());
  }

  private static BonsaiWorldState newEmptyWorldState(
      final DataStorageConfiguration dataStorageConfiguration, final BonsaiCodeCache codeCache) {
    final BonsaiWorldStateKeyValueStorage storage =
        new BonsaiWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            dataStorageConfiguration);
    return new BonsaiWorldState(
        storage,
        new NoOpBonsaiCachedMerkleTrieLoader(),
        new NoOpBonsaiWorldStateCacheManager(storage, EvmConfiguration.DEFAULT, codeCache),
        new NoOpTrieLogManager(),
        EvmConfiguration.DEFAULT,
        createStatefulConfigWithTrie(),
        codeCache);
  }
}
