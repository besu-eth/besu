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
package org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiPartialFlatDbStrategy;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.ImmutablePathBasedExtraStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.PathBasedExtraStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FlatDbStrategyProviderTest {
  private final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
      new BonsaiFlatDbStrategyProvider(
          new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_CONFIG);
  private final BonsaiFlatDbStrategyProvider archiveFlatDbStrategyProvider =
      new BonsaiFlatDbStrategyProvider(
          new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG);
  private final SegmentedKeyValueStorage composedWorldStateStorage =
      new SegmentedInMemoryKeyValueStorage(
          List.of(
              KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
              KeyValueSegmentIdentifier.CODE_STORAGE));

  @ParameterizedTest
  @EnumSource(FlatDbMode.class)
  void loadsFlatDbStrategyForStoredFlatDbMode(final FlatDbMode flatDbMode) {
    updateFlatDbMode(flatDbMode);

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.getFlatDbMode()).isEqualTo(flatDbMode);
  }

  @Test
  void loadsPartialFlatDbStrategyWhenNoFlatDbModeStored() {
    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);
  }

  @Test
  void upgradesFlatDbStrategyToFullFlatDbMode() {
    updateFlatDbMode(FlatDbMode.PARTIAL);

    flatDbStrategyProvider.upgradeToFullFlatDbMode(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy).isNotNull();
    assertThat(flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage))
        .isInstanceOf(BonsaiFullFlatDbStrategy.class);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(CodeHashCodeStorageStrategy.class);
  }

  @Test
  void upgradesFlatDbStrategyToArchiveFlatDbMode() {
    updateFlatDbMode(FlatDbMode.PARTIAL);

    archiveFlatDbStrategyProvider.upgradeToArchiveFlatDbMode(composedWorldStateStorage);
    assertThat(archiveFlatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.ARCHIVE);
    assertThat(archiveFlatDbStrategyProvider.flatDbStrategy).isNotNull();
    assertThat(archiveFlatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage))
        .isInstanceOf(BonsaiArchiveFlatDbStrategy.class);
    assertThat(archiveFlatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(CodeHashCodeStorageStrategy.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyDbCreatesFlatDbStrategyUsingCodeByHashConfig(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(PathBasedExtraStorageConfiguration.DEFAULT_MAX_LAYERS_TO_LOAD)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    final Class<? extends CodeStorageStrategy> expectedCodeStorageClass =
        codeByHashEnabled
            ? CodeHashCodeStorageStrategy.class
            : AccountHashCodeStorageStrategy.class;
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(expectedCodeStorageClass);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void emptyDbCreatesArchiveFlatDbStrategyUsingCodeByHashConfig(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();
    final FlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    final Class<? extends CodeStorageStrategy> expectedCodeStorageClass =
        codeByHashEnabled
            ? CodeHashCodeStorageStrategy.class
            : AccountHashCodeStorageStrategy.class;
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(expectedCodeStorageClass);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void existingAccountHashDbUsesAccountHash(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(PathBasedExtraStorageConfiguration.DEFAULT_MAX_LAYERS_TO_LOAD)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    final AccountHashCodeStorageStrategy accountHashCodeStorageStrategy =
        new AccountHashCodeStorageStrategy();
    // key representing account hash just needs to not be the code hash
    final Hash accountHash = Hash.wrap(Bytes32.fromHexString("0001"));
    accountHashCodeStorageStrategy.putFlatCode(
        composedWorldStateStorage, transaction, accountHash, null, Bytes.of(2));
    transaction.commit();

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(AccountHashCodeStorageStrategy.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void existingAccountHashArchiveDbUsesAccountHash(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();

    final FlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    final AccountHashCodeStorageStrategy accountHashCodeStorageStrategy =
        new AccountHashCodeStorageStrategy();
    // key representing account hash just needs to not be the code hash
    final Hash accountHash = Hash.wrap(Bytes32.fromHexString("0001"));
    accountHashCodeStorageStrategy.putFlatCode(
        composedWorldStateStorage, transaction, accountHash, null, Bytes.of(2));
    transaction.commit();

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(AccountHashCodeStorageStrategy.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void existingCodeHashDbUsesCodeHash(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.BONSAI)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(PathBasedExtraStorageConfiguration.DEFAULT_MAX_LAYERS_TO_LOAD)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();
    final BonsaiFlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();

    final CodeHashCodeStorageStrategy codeHashCodeStorageStrategy =
        new CodeHashCodeStorageStrategy();
    codeHashCodeStorageStrategy.putFlatCode(
        composedWorldStateStorage, transaction, null, Hash.hash(Bytes.of(1)), Bytes.of(1));
    transaction.commit();

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(CodeHashCodeStorageStrategy.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void existingCodeHashArchiveDbUsesCodeHash(final boolean codeByHashEnabled) {
    final DataStorageConfiguration dataStorageConfiguration =
        ImmutableDataStorageConfiguration.builder()
            .dataStorageFormat(DataStorageFormat.X_BONSAI_ARCHIVE)
            .pathBasedExtraStorageConfiguration(
                ImmutablePathBasedExtraStorageConfiguration.builder()
                    .maxLayersToLoad(3L)
                    .limitTrieLogsEnabled(true)
                    .unstable(
                        ImmutablePathBasedExtraStorageConfiguration.PathBasedUnstable.builder()
                            .codeStoredByCodeHashEnabled(codeByHashEnabled)
                            .build())
                    .build())
            .build();

    final FlatDbStrategyProvider flatDbStrategyProvider =
        new BonsaiFlatDbStrategyProvider(new NoOpMetricsSystem(), dataStorageConfiguration);

    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();

    final CodeHashCodeStorageStrategy codeHashCodeStorageStrategy =
        new CodeHashCodeStorageStrategy();
    codeHashCodeStorageStrategy.putFlatCode(
        composedWorldStateStorage, transaction, null, Hash.hash(Bytes.of(1)), Bytes.of(1));
    transaction.commit();

    flatDbStrategyProvider.loadFlatDbStrategy(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.FULL);
    assertThat(flatDbStrategyProvider.flatDbStrategy.codeStorageStrategy)
        .isInstanceOf(CodeHashCodeStorageStrategy.class);
  }

  @Test
  void downgradesFlatDbStrategyToPartiallyFlatDbMode() {
    updateFlatDbMode(FlatDbMode.FULL);

    flatDbStrategyProvider.downgradeToPartialFlatDbMode(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.PARTIAL);
    assertThat(flatDbStrategyProvider.flatDbStrategy).isNotNull();
    assertThat(flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage))
        .isInstanceOf(BonsaiPartialFlatDbStrategy.class);
  }

  @Test
  void downgradesArchiveFlatDbStrategyToPartiallyFlatDbMode() {
    updateFlatDbMode(FlatDbMode.ARCHIVE);

    flatDbStrategyProvider.downgradeToPartialFlatDbMode(composedWorldStateStorage);
    assertThat(flatDbStrategyProvider.flatDbMode).isEqualTo(FlatDbMode.PARTIAL);
    assertThat(flatDbStrategyProvider.flatDbStrategy).isNotNull();
    assertThat(flatDbStrategyProvider.getFlatDbStrategy(composedWorldStateStorage))
        .isInstanceOf(BonsaiPartialFlatDbStrategy.class);
  }

  private void updateFlatDbMode(final FlatDbMode flatDbMode) {
    final SegmentedKeyValueStorageTransaction transaction =
        composedWorldStateStorage.startTransaction();
    transaction.put(
        KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
        FlatDbStrategyProvider.FLAT_DB_MODE,
        flatDbMode.getVersion().toArrayUnsafe());
    transaction.commit();
  }
}
