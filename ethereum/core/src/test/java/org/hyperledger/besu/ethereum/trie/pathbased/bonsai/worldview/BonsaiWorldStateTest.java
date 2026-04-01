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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.WorldStateConfig.createStatefulConfigWithTrie;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BonsaiWorldStateTest {
  @Mock BonsaiWorldStateUpdateAccumulator bonsaiWorldStateUpdateAccumulator;
  @Mock BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater;
  @Mock Blockchain blockchain;
  @Mock BonsaiWorldStateKeyValueStorage bonsaiWorldStateKeyValueStorage;

  private static final Bytes CODE = Bytes.of(10);
  private static final Hash CODE_HASH = Hash.hash(CODE);
  private static final Hash ACCOUNT_HASH = Hash.hash(Address.ZERO.getBytes());
  private static final Address ACCOUNT = Address.ZERO;

  private BonsaiWorldState worldState;

  @BeforeEach
  void setup() {
    worldState =
        new BonsaiWorldState(
            InMemoryKeyValueStorageProvider.createBonsaiInMemoryWorldStateArchive(blockchain),
            bonsaiWorldStateKeyValueStorage,
            EvmConfiguration.DEFAULT,
            createStatefulConfigWithTrie(),
            new CodeCache());
  }

  @ParameterizedTest
  @MethodSource("priorAndUpdatedEmptyAndNullBytes")
  void codeUpdateDoesNothingWhenMarkedAsDeletedButAlreadyDeleted(
      final Bytes prior, final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(prior, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @Test
  void codeUpdateDoesNothingWhenAddingSameAsExistingValue() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(CODE, CODE));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @ParameterizedTest
  @MethodSource("emptyAndNullBytes")
  void removesCodeWhenMarkedAsDeleted(final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(CODE, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).removeCode(ACCOUNT_HASH, CODE_HASH);
  }

  @ParameterizedTest
  @MethodSource("codeValueAndEmptyAndNullBytes")
  void addsCodeForNewCodeValue(final Bytes prior) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(ACCOUNT, new PathBasedValue<>(prior, CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).putCode(ACCOUNT_HASH, CODE_HASH, CODE);
  }

  @Test
  void updateCodeForMultipleValues() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate = new HashMap<>();
    codeToUpdate.put(Address.fromHexString("0x1"), new PathBasedValue<>(null, CODE));
    codeToUpdate.put(Address.fromHexString("0x2"), new PathBasedValue<>(CODE, null));
    codeToUpdate.put(Address.fromHexString("0x3"), new PathBasedValue<>(Bytes.of(9), CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    worldState.updateCode(Optional.of(bonsaiUpdater), bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).putCode(Address.fromHexString("0x1").addressHash(), CODE_HASH, CODE);
    verify(bonsaiUpdater).removeCode(Address.fromHexString("0x2").addressHash(), CODE_HASH);
    verify(bonsaiUpdater).putCode(Address.fromHexString("0x3").addressHash(), CODE_HASH, CODE);
  }

  private static Stream<Bytes> emptyAndNullBytes() {
    return Stream.of(Bytes.EMPTY, null);
  }

  private static Stream<Bytes> codeValueAndEmptyAndNullBytes() {
    return Stream.of(Bytes.EMPTY, null);
  }

  private static Stream<Arguments> priorAndUpdatedEmptyAndNullBytes() {
    return Stream.of(
        Arguments.of(null, Bytes.EMPTY),
        Arguments.of(Bytes.EMPTY, null),
        Arguments.of(null, null),
        Arguments.of(Bytes.EMPTY, Bytes.EMPTY));
  }

  @Test
  void incrementBytes32_returnsNextValue() {
    assertThat(BonsaiWorldState.incrementBytes32(Bytes32.ZERO)).hasValue(UInt256.ONE);
  }

  @Test
  void incrementBytes32_returnsEmpty_whenMaxValue() {
    assertThat(BonsaiWorldState.incrementBytes32(UInt256.MAX_VALUE)).isEmpty();
  }
}
