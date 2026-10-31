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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAccountLookup;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * The state changes of a block's transactions that ran in parallel, kept until they are imported
 * into the block's world state together. Importing them one transaction at a time on the thread
 * that processes the block takes longer than running the transactions once they run in parallel,
 * and it reads every account from the database on that thread. Instead the changes are grouped by
 * account and the groups are imported in parallel, each account's changes in transaction order,
 * reading the accounts through the block access list's lookup.
 */
final class PendingStateChanges {

  // Each group holds the changes of some accounts, by account. They are grouped as the
  // transactions finish, on the thread that waits for them, rather than when they are imported.
  private final List<Map<Address, List<PartialBlockAccessView.AccountChanges>>> groups;

  PendingStateChanges(final int groupCount) {
    groups = new ArrayList<>(groupCount);
    for (int i = 0; i < groupCount; i++) {
      groups.add(new HashMap<>());
    }
  }

  /** Adds a transaction's changes, which must come after those of the transactions added so far. */
  void add(final PartialBlockAccessView partialView) {
    for (final PartialBlockAccessView.AccountChanges accountChanges :
        partialView.accountChanges()) {
      final Address address = accountChanges.getAddress();
      groups
          .get(Math.floorMod(address.hashCode(), groups.size()))
          .computeIfAbsent(address, __ -> new ArrayList<>(1))
          .add(accountChanges);
    }
  }

  /**
   * Imports and commits the changes added so far into the accumulator, one task per group on the
   * executor, and waits for all of them. The first failure is rethrown once every task is done, so
   * that a reset of the accumulator cannot run while others still write to it.
   */
  void importInto(
      final PathBasedWorldStateUpdateAccumulator<?> accumulator,
      final boolean clearEmptyAccounts,
      final BlockAccessListAccountLookup accountLookup,
      final Executor executor) {
    final List<CompletableFuture<Void>> imports = new ArrayList<>(groups.size());
    for (final Map<Address, List<PartialBlockAccessView.AccountChanges>> group : groups) {
      if (!group.isEmpty()) {
        imports.add(
            CompletableFuture.runAsync(
                () ->
                    accumulator.importAndCommitAccountChanges(
                        group, clearEmptyAccounts, accountLookup),
                executor));
      }
    }
    try {
      CompletableFuture.allOf(imports.toArray(CompletableFuture[]::new)).join();
    } catch (final CompletionException e) {
      if (e.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      throw e;
    } finally {
      groups.forEach(Map::clear);
    }
  }
}
