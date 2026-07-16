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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.staterootcommitter.DefaultStateRootCommitter;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BonsaiWorldStateTest {
  @Mock BonsaiWorldState bonsaiWorldState;
  @Mock BonsaiWorldStateUpdateAccumulator bonsaiWorldStateUpdateAccumulator;
  @Mock BonsaiWorldStateKeyValueStorage.Updater bonsaiUpdater;
  @Mock MerkleTrie<Bytes, Bytes> accountTrie;

  private static final Bytes CODE = Bytes.of(10);
  private static final Hash CODE_HASH = Hash.hash(CODE);
  private static final Hash ACCOUNT_HASH = Address.ZERO.addressHash();
  private static final Address ACCOUNT = Address.ZERO;

  private final DefaultStateRootCommitter committer = new DefaultStateRootCommitter();

  private void applyCodeUpdate(final BonsaiWorldStateUpdateAccumulator accumulator) {
    when(accumulator.getAccountsToUpdate()).thenReturn(Map.of());
    when(accumulator.getStorageToUpdate()).thenReturn(Map.of());
    when(accumulator.getStorageToClear()).thenReturn(Set.of());
    when(bonsaiWorldState.isStorageFrozen()).thenReturn(false);
    when(bonsaiWorldState.createAccountStateTrie()).thenReturn(accountTrie);
    when(accountTrie.getRootHash()).thenReturn(Bytes32.ZERO);
    doAnswer(invocation -> null).when(accountTrie).commit(any());

    committer.compute(bonsaiWorldState, null, accumulator).applyTo(bonsaiUpdater);
  }

  @ParameterizedTest
  @MethodSource("priorAndUpdatedEmptyAndNullBytes")
  void codeUpdateDoesNothingWhenMarkedAsDeletedButAlreadyDeleted(
      final Bytes prior, final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(prior, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    applyCodeUpdate(bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @Test
  void codeUpdateDoesNothingWhenAddingSameAsExistingValue() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(Address.ZERO, new PathBasedValue<>(CODE, CODE));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    applyCodeUpdate(bonsaiWorldStateUpdateAccumulator);

    verifyNoInteractions(bonsaiUpdater);
  }

  @ParameterizedTest
  @MethodSource("emptyAndNullBytes")
  void removesCodeWhenMarkedAsDeleted(final Bytes updated) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(
            Address.ZERO,
            updated == null
                ? new PathBasedValue<>(CODE, null, true)
                : new PathBasedValue<>(CODE, updated));
    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    applyCodeUpdate(bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).removeCode(ACCOUNT_HASH, CODE_HASH);
  }

  @ParameterizedTest
  @MethodSource("codeValueAndEmptyAndNullBytes")
  void addsCodeForNewCodeValue(final Bytes prior) {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate =
        Map.of(ACCOUNT, new PathBasedValue<>(prior, CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    applyCodeUpdate(bonsaiWorldStateUpdateAccumulator);

    verify(bonsaiUpdater).putCode(ACCOUNT_HASH, CODE_HASH, CODE);
  }

  @Test
  void updateCodeForMultipleValues() {
    final Map<Address, PathBasedValue<Bytes>> codeToUpdate = new HashMap<>();
    codeToUpdate.put(Address.fromHexString("0x1"), new PathBasedValue<>(null, CODE));
    codeToUpdate.put(Address.fromHexString("0x2"), new PathBasedValue<>(CODE, null, true));
    codeToUpdate.put(Address.fromHexString("0x3"), new PathBasedValue<>(Bytes.of(9), CODE));

    when(bonsaiWorldStateUpdateAccumulator.getCodeToUpdate()).thenReturn(codeToUpdate);
    applyCodeUpdate(bonsaiWorldStateUpdateAccumulator);

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
}
