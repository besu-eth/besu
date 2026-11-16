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
package org.hyperledger.besu.ethereum.eth.transactions.sorter;

import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.evm.account.Account;

import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PendingTransactionsForSender {
  private final NavigableMap<Long, PendingTransaction> pendingTransactions;
  private OptionalLong nextGap = OptionalLong.empty();

  private volatile Optional<Account> maybeSenderAccount;

  public PendingTransactionsForSender(final Optional<Account> maybeSenderAccount) {
    this.pendingTransactions = new TreeMap<>();
    this.maybeSenderAccount = maybeSenderAccount;
  }

  public void trackPendingTransaction(final PendingTransaction pendingTransaction) {
    final long nonce = pendingTransaction.getNonce();
    synchronized (pendingTransactions) {
      if (!pendingTransactions.isEmpty()) {
        final long expectedNext = pendingTransactions.lastKey() + 1;
        if (Long.compareUnsigned(nonce, expectedNext) > 0 && nextGap.isEmpty()) {
          nextGap = OptionalLong.of(expectedNext);
        }
      }
      pendingTransactions.put(nonce, pendingTransaction);
      if (nonce == nextGap.orElse(-1)) {
        findGap();
      }
    }
  }

  public void removeTrackedPendingTransaction(final PendingTransaction pendingTransaction) {
    final long nonce = pendingTransaction.getNonce();
    synchronized (pendingTransactions) {
      // must be checked before the removal: comparing with the first key after the removal is
      // always true when removing the head, and rescanning all the txs of the sender for each
      // confirmed tx makes the block added processing quadratic in the txs per sender
      final boolean wasFirst =
          !pendingTransactions.isEmpty() && pendingTransactions.firstKey() == nonce;
      // check the value when removing, because it could have been replaced
      if (pendingTransactions.remove(nonce, pendingTransaction)) {
        if (pendingTransactions.isEmpty()) {
          nextGap = OptionalLong.empty();
        } else if (!wasFirst
            || (nextGap.isPresent() && nextGap.getAsLong() <= pendingTransactions.firstKey())) {
          // removing the head of a contiguous run does not move the first gap after it
          findGap();
        }
      }
    }
  }

  public void updateSenderAccount(final Optional<Account> maybeSenderAccount) {
    this.maybeSenderAccount = maybeSenderAccount;
  }

  PendingTransactions.Status getStatus() {
    synchronized (pendingTransactions) {
      if (nextGap.isPresent()) {
        final long gap = nextGap.getAsLong();
        final long accountNonce = maybeSenderAccount.map(Account::getNonce).orElse(0L);
        final long nonGapped = Math.max(0, gap - accountNonce);
        return new PendingTransactions.Status(nonGapped, transactionCount() - nonGapped);
      }
      return new PendingTransactions.Status(transactionCount(), 0);
    }
  }

  private void findGap() {
    // find first gap
    long expectedValue = pendingTransactions.firstKey();
    for (final Long nonce : pendingTransactions.keySet()) {
      if (expectedValue == nonce) {
        // no gap, keep moving
        expectedValue++;
      } else {
        nextGap = OptionalLong.of(expectedValue);
        return;
      }
    }
    nextGap = OptionalLong.empty();
  }

  public OptionalLong maybeNextNonce() {
    if (pendingTransactions.isEmpty()) {
      return OptionalLong.empty();
    } else {
      try {
        return nextGap.isEmpty() ? OptionalLong.of(pendingTransactions.lastKey() + 1) : nextGap;
      } catch (NoSuchElementException nse) {
        // There is a timing condition where isEmpty() returns false, then another thread removes
        // the last key before our call to lastKey(). There's no benefit to us adding a mutex to
        // prevent the other thread updating the map so we just catch the exception and treat the
        // map as empty.
        return OptionalLong.empty();
      }
    }
  }

  public OptionalLong maybeCurrentNonce() {
    return maybeSenderAccount
        .map(account -> OptionalLong.of(account.getNonce()))
        .orElse(OptionalLong.empty());
  }

  public int transactionCount() {
    return pendingTransactions.size();
  }

  public List<PendingTransaction> getPendingTransactions(final long startingNonce) {
    return List.copyOf(pendingTransactions.tailMap(startingNonce).values());
  }

  public List<PendingTransaction> getPendingTransactions() {
    return List.copyOf(pendingTransactions.values());
  }

  public Stream<PendingTransaction> streamPendingTransactions() {
    return pendingTransactions.values().stream();
  }

  /**
   * Stream the pending txs that precede the first nonce gap, the ones after it cannot be included
   * in the next block, since the missing nonce is not in the pool.
   *
   * @return the pending txs before the first nonce gap, ordered by nonce
   */
  public Stream<PendingTransaction> streamPendingTransactionsBeforeGap() {
    synchronized (pendingTransactions) {
      final OptionalLong gap = nextGap;
      if (gap.isPresent()
          && !pendingTransactions.isEmpty()
          && gap.getAsLong() > pendingTransactions.firstKey()) {
        return List.copyOf(pendingTransactions.headMap(gap.getAsLong(), false).values()).stream();
      }
      return List.copyOf(pendingTransactions.values()).stream();
    }
  }

  public PendingTransaction getPendingTransactionForNonce(final long nonce) {
    return pendingTransactions.get(nonce);
  }

  public String toTraceLog() {
    return "{"
        + "senderAccount "
        + maybeSenderAccount
        + ", pendingTransactions "
        + pendingTransactions.entrySet().stream()
            .map(e -> "(" + e.getKey() + ")" + e.getValue().toTraceLog())
            .collect(Collectors.joining("; "))
        + ", nextGap "
        + nextGap
        + '}';
  }
}
