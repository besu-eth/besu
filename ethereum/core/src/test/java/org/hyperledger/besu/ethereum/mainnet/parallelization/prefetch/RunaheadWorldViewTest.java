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
package org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.code.BonsaiCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.RunaheadCalls;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunaheadWorldViewTest {

  private static final Address ADDRESS = Address.fromHexString("0x1234");
  private static final StorageSlotKey SLOT = new StorageSlotKey(UInt256.valueOf(7));

  private final BonsaiWorldState worldState = mock(BonsaiWorldState.class);
  private final BonsaiWorldStateKeyValueStorage storage =
      mock(BonsaiWorldStateKeyValueStorage.class);
  private final BonsaiCodeCache codeCache = new BonsaiCodeCache();
  private RunaheadPrefetcher.Loads loads;
  private RunaheadWorldView view;

  @BeforeEach
  void setUp() {
    when(worldState.getWorldStateStorage()).thenReturn(storage);
    when(worldState.codeCache()).thenReturn(codeCache);
    loads = new RunaheadPrefetcher.Loads(worldState);
    view = new RunaheadWorldView(worldState, loads, Optional.of(UInt256.ONE));
  }

  @AfterEach
  void tearDown() {
    RunaheadCalls.CURRENT.remove();
  }

  @Test
  void aSlotInMemoryReadsAsItsValueWithoutALoad() {
    when(storage.peekStorageValueByStorageSlotKey(ADDRESS.addressHash(), SLOT))
        .thenReturn(Optional.of(Optional.of(Bytes.of(5))));

    assertThat(view.getStorageValueByStorageSlotKey(ADDRESS, SLOT)).contains(UInt256.valueOf(5));
    verify(worldState, never()).getStorageValueByStorageSlotKey(any(), any());
  }

  @Test
  void aSlotKnownToBeEmptyReadsAsEmpty() {
    when(storage.peekStorageValueByStorageSlotKey(ADDRESS.addressHash(), SLOT))
        .thenReturn(Optional.of(Optional.empty()));

    assertThat(view.getStorageValueByStorageSlotKey(ADDRESS, SLOT)).isEmpty();
  }

  @Test
  void aSlotNotInMemoryReadsAsThePlaceholderAndIsLoadedInTheBackground() {
    when(storage.peekStorageValueByStorageSlotKey(ADDRESS.addressHash(), SLOT))
        .thenReturn(Optional.empty());

    assertThat(view.getStorageValueByStorageSlotKey(ADDRESS, SLOT)).contains(UInt256.ONE);
    verify(worldState, timeout(5_000)).getStorageValueByStorageSlotKey(ADDRESS, SLOT);
  }

  @Test
  void aSlotIsLoadedOnceHoweverOftenItIsRead() {
    when(storage.peekStorageValueByStorageSlotKey(ADDRESS.addressHash(), SLOT))
        .thenReturn(Optional.empty());

    view.getStorageValueByStorageSlotKey(ADDRESS, SLOT);
    view.getStorageValueByStorageSlotKey(ADDRESS, SLOT);

    verify(worldState, timeout(5_000).times(1)).getStorageValueByStorageSlotKey(ADDRESS, SLOT);
  }

  @Test
  void anAccountOutsideACallTargetLookupIsReadFromTheWorldState() {
    final Account account = mock(Account.class);
    when(worldState.get(ADDRESS)).thenReturn(account);

    assertThat(view.get(ADDRESS)).isSameAs(account);
  }

  @Test
  void aCallTargetNotInMemoryReadsAsAPlaceholderWhileItLoads() {
    when(storage.peekAccount(ADDRESS.addressHash())).thenReturn(Optional.empty());
    final RunaheadCalls calls = mock(RunaheadCalls.class);
    when(calls.isLookingUpCallTarget()).thenReturn(true);
    RunaheadCalls.Switch.enable();
    RunaheadCalls.CURRENT.set(calls);

    final Account target = view.get(ADDRESS);

    assertThat(RunaheadCalls.isPlaceholder(target)).isTrue();
    verify(worldState, timeout(5_000)).loadAccountAndTriePath(ADDRESS);
    // reading its code must not put the placeholder's code into the code cache all executions use
    assertThat(target.getCode()).isEqualTo(Bytes.of(0));
    assertThat(codeCache.getIfPresent(RunaheadCalls.PLACEHOLDER_CODE_HASH)).isNull();
  }

  @Test
  void theCodeOfAPlaceholderIsAStopThatNeverReachesTheSharedCodeCache() {
    assertThat(view.getCode(ADDRESS, RunaheadCalls.PLACEHOLDER_CODE_HASH))
        .hasValueSatisfying(code -> assertThat(code.getBytes()).isEqualTo(Bytes.of(0)));
    assertThat(codeCache.getIfPresent(RunaheadCalls.PLACEHOLDER_CODE_HASH)).isNull();
  }
}
