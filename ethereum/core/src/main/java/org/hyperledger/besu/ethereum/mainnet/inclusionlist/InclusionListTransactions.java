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
package org.hyperledger.besu.ethereum.mainnet.inclusionlist;

import static org.hyperledger.besu.crypto.Hash.keccak256;

import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * The transactions of an inclusion list (EIP-7805), with a checksum that identifies them.
 *
 * <p>The transactions have no particular order, and a transaction is identified by its exact bytes,
 * so they are deduplicated and sorted by hash: {@link Transaction#equals} ignores some fields, like
 * the access list and the authorization list. The checksum is the hash of the sorted transaction
 * hashes, so it does not depend on the order of the transactions, and it cannot be steered by
 * grinding transaction hashes, as a sum of them could.
 */
public final class InclusionListTransactions {
  /** Orders and deduplicates inclusion list transactions by hash. */
  public static final Comparator<Transaction> HASH_ORDER =
      Comparator.comparing((Transaction tx) -> tx.getHash().getBytes());

  /** An empty inclusion list. */
  public static final InclusionListTransactions EMPTY = new InclusionListTransactions(List.of());

  private final SortedSet<Transaction> transactions;
  private final Bytes32 checksum;

  /**
   * Instantiates the inclusion list with the given transactions, deduplicating them by hash.
   *
   * @param transactions the transactions of the inclusion list, in any order
   */
  public InclusionListTransactions(final Collection<Transaction> transactions) {
    final SortedSet<Transaction> sortedTransactions = new TreeSet<>(HASH_ORDER);
    sortedTransactions.addAll(transactions);
    this.transactions = Collections.unmodifiableSortedSet(sortedTransactions);
    this.checksum =
        keccak256(
            Bytes.concatenate(
                sortedTransactions.stream().map(tx -> tx.getHash().getBytes()).toList()));
  }

  /**
   * Returns the transactions, deduplicated and sorted by hash.
   *
   * @return the transactions of the inclusion list
   */
  public Set<Transaction> getTransactions() {
    return transactions;
  }

  /**
   * Returns the checksum that identifies the transactions of this inclusion list.
   *
   * @return the hash of the sorted transaction hashes
   */
  public Bytes32 getChecksum() {
    return checksum;
  }

  /**
   * Whether the inclusion list has no transactions.
   *
   * @return true if the inclusion list is empty
   */
  public boolean isEmpty() {
    return transactions.isEmpty();
  }

  @Override
  public boolean equals(final Object other) {
    return other instanceof InclusionListTransactions that && checksum.equals(that.checksum);
  }

  @Override
  public int hashCode() {
    return checksum.hashCode();
  }

  @Override
  public String toString() {
    return "InclusionListTransactions{size=" + transactions.size() + ", checksum=" + checksum + "}";
  }
}
