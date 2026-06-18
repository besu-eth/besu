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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.bal;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.CodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldView;
import org.hyperledger.besu.evm.internal.EvmConfiguration;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BonsaiBalAccountTest {

  private static final Address ADDRESS =
      Address.fromHexString("0x00000000000000000000000000000000000000aa");

  @Test
  void getNonceReadsBalWithoutLoadingParent() {
    final AtomicInteger parentLoads = new AtomicInteger();
    final PathBasedWorldView parentView = mockParentView(parentLoads);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            dummyUpdater(parentView),
            ADDRESS,
            () -> parentView.get(ADDRESS),
            balViewWithNonce(7L),
            1L,
            new CodeCache());

    assertThat(account.getNonce()).isEqualTo(7L);
    assertThat(parentLoads).hasValue(0);
  }

  @Test
  void getBalanceReadsBalWithoutLoadingParent() {
    final Wei balBalance = Wei.of(42_000);
    final AtomicInteger parentLoads = new AtomicInteger();
    final PathBasedWorldView parentView = mockParentView(parentLoads);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            dummyUpdater(parentView),
            ADDRESS,
            () -> parentView.get(ADDRESS),
            balViewWithBalance(balBalance),
            1L,
            new CodeCache());

    assertThat(account.getBalance()).isEqualTo(balBalance);
    assertThat(parentLoads).hasValue(0);
  }

  @Test
  void isStorageEmptyLoadsParentWhenNotInBal() {
    final AtomicInteger parentLoads = new AtomicInteger();
    final PathBasedWorldView parentView = mockParentView(parentLoads);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            dummyUpdater(parentView),
            ADDRESS,
            () -> parentView.get(ADDRESS),
            balViewWithNonce(1L),
            1L,
            new CodeCache());

    assertThat(account.isStorageEmpty()).isTrue();
    assertThat(parentLoads).hasValue(1);
  }

  @Test
  void existsUsesBalWithoutLoadingParent() {
    final PathBasedWorldView parentView =
        Mockito.mock(PathBasedWorldView.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(parentView.get(ADDRESS)).thenReturn(null);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            dummyUpdater(parentView),
            ADDRESS,
            () -> parentView.get(ADDRESS),
            balViewWithNonce(2L),
            1L,
            new CodeCache());

    assertThat(account.exists()).isTrue();
    Mockito.verify(parentView, Mockito.never()).get(ADDRESS);
  }

  @Test
  void missingAccountReturnsDefaultsWithoutBal() {
    final PathBasedWorldView parentView =
        Mockito.mock(PathBasedWorldView.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(parentView.get(ADDRESS)).thenReturn(null);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            dummyUpdater(parentView),
            ADDRESS,
            () -> parentView.get(ADDRESS),
            BlockAccessListAddressView.of(new BlockAccessList(List.of())),
            1L,
            new CodeCache());

    assertThat(account.exists()).isFalse();
    assertThat(account.getNonce()).isZero();
    assertThat(account.getBalance()).isEqualTo(Wei.ZERO);
    assertThat(account.getCodeHash()).isEqualTo(Hash.EMPTY);
  }

  @Test
  void copyLazyPreservesBalWithoutLoadingParent() {
    final AtomicInteger parentLoads = new AtomicInteger();
    final PathBasedWorldView parentView = mockParentView(parentLoads);
    final BonsaiBalWorldStateUpdateAccumulator updater = dummyUpdater(parentView);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            updater,
            ADDRESS,
            () -> parentView.get(ADDRESS),
            balViewWithNonce(7L),
            1L,
            new CodeCache());

    final BonsaiBalAccount copied = new BonsaiBalAccount(account, updater, true);

    assertThat(copied).isNotSameAs(account);
    assertThat(copied.getNonce()).isEqualTo(7L);
    assertThat(parentLoads).hasValue(0);
  }

  @Test
  void copyLazyReusesCachedParentWithoutSecondDbRead() {
    final AtomicInteger parentLoads = new AtomicInteger();
    final PathBasedWorldView parentView = mockParentView(parentLoads);
    final BonsaiBalWorldStateUpdateAccumulator updater = dummyUpdater(parentView);

    final BonsaiBalAccount account =
        new BonsaiBalAccount(
            updater,
            ADDRESS,
            () -> parentView.get(ADDRESS),
            BlockAccessListAddressView.of(new BlockAccessList(List.of())),
            1L,
            new CodeCache());

    assertThat(account.getNonce()).isZero();
    assertThat(parentLoads).hasValue(1);

    final BonsaiBalAccount copied = new BonsaiBalAccount(account, updater, true);
    assertThat(copied.getNonce()).isZero();
    assertThat(parentLoads).hasValue(1);
  }

  private static BonsaiBalWorldStateUpdateAccumulator dummyUpdater(
      final PathBasedWorldView parentView) {
    return new BonsaiBalWorldStateUpdateAccumulator(
        parentView,
        EvmConfiguration.DEFAULT,
        new CodeCache(),
        new org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListOverlay(
            BlockAccessListAddressView.of(new BlockAccessList(List.of())), 1L));
  }

  private static PathBasedWorldView mockParentView(final AtomicInteger parentLoads) {
    final PathBasedWorldView parentView =
        Mockito.mock(PathBasedWorldView.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(parentView.get(ADDRESS))
        .thenAnswer(
            invocation -> {
              parentLoads.incrementAndGet();
              return null;
            });
    return parentView;
  }

  private static BlockAccessListAddressView balViewWithNonce(final long nonce) {
    return BlockAccessListAddressView.of(
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(),
                    List.of(new NonceChange(0, nonce)),
                    List.of()))));
  }

  private static BlockAccessListAddressView balViewWithBalance(final Wei balance) {
    return BlockAccessListAddressView.of(
        new BlockAccessList(
            List.of(
                new AccountChanges(
                    ADDRESS,
                    List.of(),
                    List.of(),
                    List.of(new BalanceChange(0, balance)),
                    List.of(),
                    List.of()))));
  }
}
