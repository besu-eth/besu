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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.plugin.services.exception.StorageException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PendingStateChangesTest {

  private static final BlockAccessListAccountLookup LOOKUP =
      BlockAccessListAccountLookup.of(new BlockAccessList(List.of()));

  private static final List<Address> ACCOUNTS =
      List.of(
          Address.fromHexString("0x01"),
          Address.fromHexString("0x02"),
          Address.fromHexString("0x03"),
          Address.fromHexString("0x04"),
          Address.fromHexString("0x05"));

  private final ExecutorService executor = Executors.newFixedThreadPool(4);
  private final PathBasedWorldStateUpdateAccumulator<?> accumulator =
      mock(PathBasedWorldStateUpdateAccumulator.class);

  @AfterEach
  void shutDown() {
    executor.shutdownNow();
  }

  @Test
  void importsEveryChangeOnceWithEachAccountsChangesInTransactionOrder() {
    final List<Map<Address, List<PartialBlockAccessView.AccountChanges>>> imported =
        Collections.synchronizedList(new ArrayList<>());
    doAnswer(
            invocation -> {
              imported.add(new HashMap<>(invocation.getArgument(0)));
              return null;
            })
        .when(accumulator)
        .importAndCommitAccountChanges(any(), anyBoolean(), any());

    final PendingStateChanges pending = new PendingStateChanges(3);
    for (int transaction = 1; transaction <= 10; transaction++) {
      pending.add(balancesOfEveryAccount(transaction));
    }
    pending.importInto(accumulator, true, LOOKUP, executor);

    assertThat(imported.stream().flatMap(group -> group.values().stream()).mapToInt(List::size))
        .containsOnly(10)
        .hasSize(ACCOUNTS.size());
    for (final Address account : ACCOUNTS) {
      final List<Map<Address, List<PartialBlockAccessView.AccountChanges>>> groupsWithAccount =
          imported.stream().filter(group -> group.containsKey(account)).toList();
      assertThat(groupsWithAccount).hasSize(1);
      assertThat(
              groupsWithAccount.getFirst().get(account).stream()
                  .map(c -> c.getPostBalance().orElseThrow()))
          .containsExactly(
              Wei.of(1),
              Wei.of(2),
              Wei.of(3),
              Wei.of(4),
              Wei.of(5),
              Wei.of(6),
              Wei.of(7),
              Wei.of(8),
              Wei.of(9),
              Wei.of(10));
    }
  }

  @Test
  void rethrowsAFailureOnceEveryGroupIsImported() {
    final AtomicInteger finished = new AtomicInteger();
    final AtomicInteger calls = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (calls.getAndIncrement() == 0) {
                throw new StorageException("Simulated failure");
              }
              Thread.sleep(50);
              finished.incrementAndGet();
              return null;
            })
        .when(accumulator)
        .importAndCommitAccountChanges(any(), anyBoolean(), any());

    final PendingStateChanges pending = new PendingStateChanges(ACCOUNTS.size());
    pending.add(balancesOfEveryAccount(1));

    assertThatThrownBy(() -> pending.importInto(accumulator, true, LOOKUP, executor))
        .isInstanceOf(StorageException.class);
    // every other group was imported before the failure was rethrown
    assertThat(calls.get()).isGreaterThan(1);
    assertThat(finished.get()).isEqualTo(calls.get() - 1);
  }

  @Test
  void importsNothingTwice() {
    final AtomicInteger imports = new AtomicInteger();
    doAnswer(
            invocation -> {
              imports.incrementAndGet();
              return null;
            })
        .when(accumulator)
        .importAndCommitAccountChanges(any(), anyBoolean(), any());

    final PendingStateChanges pending = new PendingStateChanges(1);
    pending.add(balancesOfEveryAccount(1));
    pending.importInto(accumulator, true, LOOKUP, Runnable::run);
    pending.importInto(accumulator, true, LOOKUP, Runnable::run);

    assertThat(imports.get()).isEqualTo(1);
  }

  private static PartialBlockAccessView balancesOfEveryAccount(final int transaction) {
    final PartialBlockAccessView.PartialBlockAccessViewBuilder builder =
        new PartialBlockAccessView.PartialBlockAccessViewBuilder().withTxIndex(transaction);
    for (final Address account : ACCOUNTS) {
      builder.getOrCreateAccountBuilder(account).withPostBalance(Wei.of(transaction));
    }
    return builder.build();
  }
}
