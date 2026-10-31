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
package org.hyperledger.besu.ethereum.mainnet.block.access.list;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.AccountChanges;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.NonceChange;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class BlockAccessListComparisonTest {

  @Test
  void listsOfManyAccountsWithTheSameChangesAreTheSame() {
    final BlockAccessList list = new BlockAccessList(accounts(1000, 1));

    assertThat(list.hasSameChangesAs(new BlockAccessList(accounts(1000, 1)))).isTrue();
  }

  @Test
  void anAccountChangedDifferentlyInALongListMakesTheListsDiffer() {
    final List<AccountChanges> other = new ArrayList<>(accounts(1000, 1));
    other.set(700, account(700, 1, 0));

    assertThat(new BlockAccessList(accounts(1000, 1)).hasSameChangesAs(new BlockAccessList(other)))
        .isFalse();
  }

  @Test
  void aChangeThatDiffersInAnAccountWithManyMakesTheListsDiffer() {
    final BlockAccessList list = new BlockAccessList(List.of(account(1, 2000, -1)));

    assertThat(list.hasSameChangesAs(new BlockAccessList(List.of(account(1, 2000, -1))))).isTrue();
    assertThat(list.hasSameChangesAs(new BlockAccessList(List.of(account(1, 2000, 1500)))))
        .isFalse();
  }

  @Test
  void listsOfDifferentLengthsDiffer() {
    assertThat(
            new BlockAccessList(accounts(600, 1))
                .hasSameChangesAs(new BlockAccessList(accounts(601, 1))))
        .isFalse();
  }

  private static List<AccountChanges> accounts(final int count, final int changes) {
    return IntStream.range(0, count).mapToObj(i -> account(i, changes, -1)).toList();
  }

  /**
   * An account with {@code changes} balance and nonce changes, the balance at {@code odd} off by
   * one, or none if it is negative.
   */
  private static AccountChanges account(final int account, final int changes, final int odd) {
    final List<BalanceChange> balances = new ArrayList<>();
    final List<NonceChange> nonces = new ArrayList<>();
    for (int i = 0; i < changes; i++) {
      balances.add(new BalanceChange(i + 1, Wei.of(1000L * account + i + (i == odd ? 1 : 0))));
      nonces.add(new NonceChange(i + 1, i + 1));
    }
    return new AccountChanges(
        Address.fromHexString(String.format("0x%040x", account)),
        List.of(),
        List.of(),
        balances,
        nonces,
        List.of());
  }
}
