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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tracks the authorities named by the authorization tuples of the code delegation transactions that
 * are pending in the pool. An account that is going to be delegated is treated by the txpool like
 * an already delegated account, since once delegated, its balance can be moved by a call from an
 * unrelated party, without consuming its nonce, so the transactions queued behind the first one
 * could become unfundable without the txpool being able to notice.
 *
 * <p>The tuples are tracked without checking if they could be applied, so this over-approximates
 * the set of accounts that are going to be delegated. The tuples that can never be applied
 * regardless of the state, because of the chain id or the nonce value, are not tracked, and the
 * ones with a nonce that is already lower than the current nonce of the account are ignored when
 * queried, since an authorization can not be replayed once its nonce has been consumed.
 */
class PendingDelegations {

  /** A pending transaction that contains authorization tuples for an authority. */
  private record DelegationTx(
      Transaction transaction, Address sender, NavigableSet<Long> tupleNonces) {}

  private final Map<Address, Map<Hash, DelegationTx>> txsByAuthority = new HashMap<>();
  private final Map<Hash, Set<Address>> authoritiesByTx = new HashMap<>();

  /**
   * Start tracking the authorities of a pending transaction, if it is a code delegation one.
   *
   * @param transaction the pending transaction
   * @param authorities the authorities of the transaction, see {@link #authoritiesOf(Transaction)}
   */
  synchronized void add(
      final Transaction transaction, final Map<Address, NavigableSet<Long>> authorities) {
    if (authorities.isEmpty()) {
      return;
    }
    final Hash hash = transaction.getHash();
    authoritiesByTx.put(hash, Set.copyOf(authorities.keySet()));
    authorities.forEach(
        (authority, tupleNonces) ->
            txsByAuthority
                .computeIfAbsent(authority, unused -> new HashMap<>())
                .put(hash, new DelegationTx(transaction, transaction.getSender(), tupleNonces)));
  }

  /**
   * Stop tracking the authorities of a transaction that is not pending anymore. It is safe to call
   * it for a transaction that is not tracked.
   *
   * @param transaction the transaction that has been removed from the pool
   */
  synchronized void remove(final Transaction transaction) {
    final Hash hash = transaction.getHash();
    final Set<Address> authorities = authoritiesByTx.remove(hash);
    if (authorities == null) {
      return;
    }
    authorities.forEach(
        authority -> {
          final var authorityTxs = txsByAuthority.get(authority);
          if (authorityTxs != null) {
            authorityTxs.remove(hash);
            if (authorityTxs.isEmpty()) {
              txsByAuthority.remove(authority);
            }
          }
        });
  }

  /**
   * The tracked transactions that delegate the account.
   *
   * @param authority the account
   * @return a snapshot of the transactions
   */
  synchronized List<Transaction> transactionsFor(final Address authority) {
    return txsByAuthority.getOrDefault(authority, Map.of()).values().stream()
        .map(DelegationTx::transaction)
        .toList();
  }

  /**
   * All the tracked transactions.
   *
   * @return a snapshot of the transactions
   */
  synchronized List<Transaction> trackedTransactions() {
    return txsByAuthority.values().stream()
        .flatMap(authorityTxs -> authorityTxs.values().stream())
        .map(DelegationTx::transaction)
        .distinct()
        .toList();
  }

  /**
   * The number of transactions that are tracked, so the ones that are pending and delegate at least
   * one authority.
   *
   * @return the number of tracked transactions
   */
  synchronized int trackedTransactionCount() {
    return authoritiesByTx.size();
  }

  /**
   * Is there a pending delegation for this account, sponsored by a transaction of another sender,
   * that could still be applied?
   *
   * @param authority the account
   * @param currentNonce the current nonce of the account, tuples with a lower nonce are ignored
   * @return true if there is at least one pending transaction from another sender that delegates
   *     the account
   */
  synchronized boolean hasSponsoredDelegation(final Address authority, final long currentNonce) {
    return txsByAuthority.getOrDefault(authority, Map.of()).values().stream()
        .anyMatch(
            delegationTx ->
                !delegationTx.sender().equals(authority)
                    && delegationTx.tupleNonces().ceiling(currentNonce) != null);
  }

  /**
   * The nonces consumed by a transaction that delegates its own sender, if it can be applied.
   *
   * <p>The nonce of the sender is incremented before the authorizations are processed, so the first
   * tuple that can be applied is the one with the nonce after the one of the transaction, and
   * applying it increments the nonce of the account again, so the tuple with the following nonce
   * can be applied too, and so on. A tuple with any other nonce is skipped, since it does not match
   * the nonce of the account when it is processed.
   *
   * @param firstConsumedNonce the first nonce consumed by the delegation, the one after the nonce
   *     of the transaction
   * @param lastConsumedNonce the last nonce consumed by the delegation, so the next one is the
   *     first that can be executed after it
   */
  record SelfDelegation(long firstConsumedNonce, long lastConsumedNonce) {

    boolean consumes(final long nonce) {
      return nonce >= firstConsumedNonce && nonce <= lastConsumedNonce;
    }

    /**
     * The highest nonce that a sender delegating itself is allowed to have pending
     *
     * @return the nonce after the last consumed one
     */
    long highestAllowedNonce() {
      return lastConsumedNonce + 1;
    }
  }

  /**
   * Find out if a transaction delegates its own sender, and which nonces it consumes.
   *
   * @param transactionNonce the nonce of the transaction
   * @param senderTupleNonces the nonces of the tuples of the transaction that name its sender
   * @return the delegation, empty if no tuple can be applied
   */
  static Optional<SelfDelegation> selfDelegationOf(
      final long transactionNonce, final NavigableSet<Long> senderTupleNonces) {
    // authoritiesOf excludes the tuple nonces that would overflow here
    if (transactionNonce < 0
        || transactionNonce >= Long.MAX_VALUE - 1
        || !senderTupleNonces.contains(transactionNonce + 1)) {
      return Optional.empty();
    }
    final long first = transactionNonce + 1;
    long last = first;
    while (senderTupleNonces.contains(last + 1)) {
      ++last;
    }
    return Optional.of(new SelfDelegation(first, last));
  }

  /**
   * The self delegations of the pending transactions of an account, that could still be executed.
   *
   * @param authority the account
   * @param currentNonce the current nonce of the account, the transactions with a lower nonce can
   *     not be executed anymore
   * @return the self delegations, ordered by the nonce that they consume
   */
  synchronized List<SelfDelegation> pendingSelfDelegations(
      final Address authority, final long currentNonce) {
    return txsByAuthority.getOrDefault(authority, Map.of()).values().stream()
        .filter(delegationTx -> delegationTx.sender().equals(authority))
        .filter(delegationTx -> delegationTx.transaction().getNonce() >= currentNonce)
        .map(
            delegationTx ->
                selfDelegationOf(delegationTx.transaction().getNonce(), delegationTx.tupleNonces()))
        .flatMap(Optional::stream)
        .sorted(Comparator.comparingLong(SelfDelegation::firstConsumedNonce))
        .toList();
  }

  /**
   * The authorities, with a recoverable address, named by the authorization tuples of a
   * transaction, with the nonces of their tuples. Tuples that can never be applied are skipped: the
   * ones with a chain id that is neither zero nor the one of the transaction, and the ones with a
   * nonce that is not lower than 2^64-1. It also recovers the authorities, so it is expensive the
   * first time it is called for a transaction, and it is better to not do it while holding the pool
   * lock.
   *
   * @param transaction the transaction
   * @return the authorities, empty if the transaction is not a code delegation one
   */
  static Map<Address, NavigableSet<Long>> authoritiesOf(final Transaction transaction) {
    final Map<Address, NavigableSet<Long>> tupleNoncesByAuthority = new HashMap<>();
    transaction
        .getCodeDelegationList()
        .ifPresent(
            codeDelegations ->
                codeDelegations.stream()
                    .filter(
                        codeDelegation ->
                            // the nonce is unsigned, so a negative value is 2^63 or higher, that
                            // is unreachable by an account nonce, and that includes 2^64-1, that
                            // can never be applied. 2^63-1 is excluded too, so that adding one to
                            // a tuple nonce can not overflow
                            codeDelegation.nonce() >= 0
                                && codeDelegation.nonce() != Long.MAX_VALUE
                                && isChainIdApplicable(codeDelegation.chainId(), transaction))
                    .forEach(
                        codeDelegation ->
                            codeDelegation
                                .authorizer()
                                .ifPresent(
                                    authority ->
                                        tupleNoncesByAuthority
                                            .computeIfAbsent(authority, unused -> new TreeSet<>())
                                            .add(codeDelegation.nonce()))));
    return tupleNoncesByAuthority;
  }

  private static boolean isChainIdApplicable(
      final BigInteger tupleChainId, final Transaction transaction) {
    return tupleChainId.signum() == 0
        || transaction.getChainId().map(tupleChainId::equals).orElse(false);
  }
}
