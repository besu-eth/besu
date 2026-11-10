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

import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.NONCE_ALREADY_CONSUMED_FOR_SENDER;
import static org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER;
import static org.hyperledger.besu.evm.worldstate.CodeDelegationHelper.hasCodeDelegation;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.AccountState;
import org.hyperledger.besu.evm.fluent.SimpleAccount;
import org.hyperledger.besu.evm.worldstate.WorldState;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Limits the pending transactions of accounts that are delegated (EIP-7702), or are going to be.
 *
 * <p>A delegated account can have its balance moved by a call from an unrelated party, without
 * consuming its nonce, so a pool that relies on the fundability of the transactions queued behind
 * the first one of a sender can not know if they are going to be fundable. This policy is for pools
 * that admit transactions from unrelated, untrusted senders, and that rely on that fundability, so
 * it is not applied unless the pool asks for it, see {@link
 * PendingTransactions#requiresDelegationLimits()}. A pool that does not rely on fundability, as a
 * gas-free FIFO pool, is not affected by it in any way.
 *
 * <p>It does not depend on the implementation of the pool, it only uses its public API, and it
 * keeps its own view of the pending delegations, that is checked against the pool before being
 * used, so it does not depend on how, or when, the pool notifies removals.
 *
 * <p>All the additions of transactions to the pool must go through {@link
 * #addTransaction(PendingTransaction, Optional, Hash)}, so the check and the addition are atomic.
 * It calls into the pool while holding its own lock, so the pool must never call back into it.
 */
class DelegationLimits {
  private static final Logger LOG = LoggerFactory.getLogger(DelegationLimits.class);

  private final PendingTransactions pendingTransactions;
  private final ProtocolContext protocolContext;
  private final PendingDelegations pendingDelegations = new PendingDelegations();

  DelegationLimits(
      final PendingTransactions pendingTransactions, final ProtocolContext protocolContext) {
    this.pendingTransactions = pendingTransactions;
    this.protocolContext = protocolContext;
  }

  /**
   * Checks the delegation limits and, if the transaction is allowed, adds it to the pool.
   *
   * @param pendingTransaction the transaction to add
   * @param validatedSenderAccount the account of the sender in the chain head state, with its code
   *     if it is delegated, empty if the account does not exist
   * @param validatedAtChainHead the hash of the chain head that the account has been read from
   * @return the result of the addition, or the rejection reason of the limits
   */
  synchronized TransactionAddedResult addTransaction(
      final PendingTransaction pendingTransaction,
      final Optional<Account> validatedSenderAccount,
      final Hash validatedAtChainHead) {

    // The account was read before taking this lock, so a block could have been imported since,
    // delegating the sender, and its purge, that takes this lock too, could have already run, so
    // nothing would limit the transactions of the sender that were validated before the block. If
    // the chain head has moved, the account is read again, while holding the lock. If it moves
    // after this check, the purge of the block runs after this addition, so it finds the
    // transaction.
    final Optional<Account> maybeSenderAccount;
    if (blockchain().getChainHeadHash().equals(validatedAtChainHead)) {
      maybeSenderAccount = validatedSenderAccount;
    } else {
      final var reread = readSenderAtChainHead(pendingTransaction.getSender());
      if (reread.isEmpty()) {
        return TransactionAddedResult.INTERNAL_ERROR;
      }
      maybeSenderAccount = reread.get();
    }

    // the authorities are computed only once, since it is not cheap for a transaction with many
    // authorization tuples
    final Map<Address, NavigableSet<Long>> authorities =
        PendingDelegations.authoritiesOf(pendingTransaction.getTransaction());

    final TransactionAddedResult rejection =
        checkLimits(pendingTransaction, authorities, maybeSenderAccount);
    if (rejection != null) {
      return rejection;
    }

    final TransactionAddedResult addResult =
        pendingTransactions.addTransaction(pendingTransaction, maybeSenderAccount);

    // it is tracked only if it is pending after the addition, since adding it could have made the
    // pool evict it right away. A transaction that stops being pending later is found out when the
    // delegations are used, or when the tracked ones are swept
    if (addResult.isSuccess()
        && pendingTransactions.containsTransaction(pendingTransaction.getTransaction())) {
      pendingDelegations.add(pendingTransaction.getTransaction(), authorities);
      // the stale entries are found out when they are used, this only bounds the ones that are not
      if (pendingDelegations.trackedTransactionCount() > pendingTransactions.size()) {
        forgetDroppedDelegations();
      }
    }
    return addResult;
  }

  /**
   * An account that is delegated can only have one pending transaction, the one with its current
   * nonce, since its balance can be moved by a call from an unrelated party, so the transactions
   * queued behind the first one could be unfundable, and the txpool can not know. The incoming
   * transactions of such an account are already limited when they are added, but when the
   * delegation arrives in a block, without having been seen by the txpool, the account could
   * already have a long queue, that is removed here, from the highest nonce, so that no gaps are
   * created. It must be called after the pool has processed the block, so the pending transactions
   * that are already confirmed or consumed by the delegation are not there anymore.
   *
   * @param blockHeader the header of the block that has been added
   * @param confirmedTransactions the transactions of the block
   */
  synchronized void blockAdded(
      final BlockHeader blockHeader, final List<Transaction> confirmedTransactions) {
    // nothing that fails here can be allowed to escape, the block event is not processed again, so
    // it would skip the purge for good, silently, and the transactions that are re-added after a
    // reorg. The removal of the transactions notifies the drop listeners, that are not ours
    try {
      purgeDelegatedAuthorities(blockHeader, confirmedTransactions);
    } catch (final Exception e) {
      LOG.error(
          "Error purging the pending transactions of the accounts delegated by block {}",
          blockHeader.toLogString(),
          e);
    }
    // the purge, and the confirmed transactions removed by the pool, leave dropped transactions
    // tracked, and this is after them, so the next block does not find them stale
    forgetDroppedDelegations();
  }

  private void purgeDelegatedAuthorities(
      final BlockHeader blockHeader, final List<Transaction> confirmedTransactions) {
    // only the authorities that have pending transactions need to be looked up in the state
    final Set<Address> authoritiesWithPendingTxs = new HashSet<>();
    confirmedTransactions.stream()
        .filter(tx -> tx.getType().supportsDelegateCode())
        .flatMap(tx -> PendingDelegations.authoritiesOf(tx).keySet().stream())
        .filter(
            authority ->
                !pendingTransactions
                    .getPendingTransactionsFor(authority)
                    .pendingTransactions()
                    .isEmpty())
        .forEach(authoritiesWithPendingTxs::add);
    if (authoritiesWithPendingTxs.isEmpty()) {
      return;
    }

    final List<PendingTransaction> toPurge = new ArrayList<>();
    try {
      final var maybeWorldState =
          protocolContext
              .getWorldStateArchive()
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(blockHeader));
      if (maybeWorldState.isEmpty()) {
        // the world state as of the block may not be reconstructable, so it can under-purge, but it
        // never purges a transaction of an account that has not been seen as delegated
        LOG.atDebug()
            .setMessage("World state at {} unavailable, pending delegations are not checked")
            .addArgument(blockHeader::toLogString)
            .log();
        return;
      }
      try (final WorldState worldState = maybeWorldState.get()) {
        for (final Address authority : authoritiesWithPendingTxs) {
          final Account account = worldState.get(authority);
          // whether the tuple of the authority was applied or not, an authority that is delegated
          // after the block is limited like any other delegated account
          if (account != null && isDelegated(account)) {
            final var pendingTxs =
                pendingTransactions.getPendingTransactionsFor(authority).pendingTransactions();
            // from the highest nonce, so no gaps are created
            for (int i = pendingTxs.size() - 1; i >= 0; --i) {
              if (pendingTxs.get(i).getNonce() <= account.getNonce()) {
                break;
              }
              toPurge.add(pendingTxs.get(i));
            }
          }
        }
      }
    } catch (final Exception e) {
      LOG.error(
          "Error reading world state at {} to check the pending transactions of delegated accounts",
          blockHeader.toLogString(),
          e);
    }

    if (!toPurge.isEmpty()) {
      pendingTransactions.removeInvalidated(toPurge);
      LOG.atDebug()
          .setMessage("Purged {} pending transaction(s) of accounts delegated by the last block")
          .addArgument(toPurge::size)
          .log();
    }
  }

  private MutableBlockchain blockchain() {
    return protocolContext.getBlockchain();
  }

  /**
   * @return the account in the state of the chain head, empty if it could not be read
   */
  private Optional<Optional<Account>> readSenderAtChainHead(final Address sender) {
    try (final WorldState worldState =
        protocolContext
            .getWorldStateArchive()
            .getWorldState(
                WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(
                    blockchain().getChainHeadHeader()))
            .orElseThrow()) {
      return Optional.of(Optional.ofNullable(worldState.get(sender)).map(DelegationLimits::detach));
    } catch (final Exception e) {
      LOG.debug(
          "Unable to read the sender {} at the chain head, the transaction is rejected", sender, e);
      return Optional.empty();
    }
  }

  /**
   * A copy of the account that is detached from the world state, and that only keeps the code of
   * the accounts that are delegated, since the txpool needs to know that, and the sender of a valid
   * transaction is either an EOA or a delegated account, so there is no contract code to copy.
   *
   * @param account the account
   * @return the detached copy
   */
  static Account detach(final Account account) {
    final SimpleAccount copy =
        new SimpleAccount(account.getAddress(), account.getNonce(), account.getBalance());
    final Bytes code = account.getCode();
    if (code != null && hasCodeDelegation(code)) {
      copy.setCode(code);
    }
    return copy;
  }

  @VisibleForTesting
  synchronized int trackedTransactionCount() {
    return pendingDelegations.trackedTransactionCount();
  }

  private void forgetDroppedDelegations() {
    pendingDelegations.trackedTransactions().stream()
        .filter(tx -> !pendingTransactions.containsTransaction(tx))
        .forEach(pendingDelegations::remove);
  }

  private void forgetDroppedDelegationsOf(final Address authority) {
    pendingDelegations.transactionsFor(authority).stream()
        .filter(tx -> !pendingTransactions.containsTransaction(tx))
        .forEach(pendingDelegations::remove);
  }

  /**
   * @return the rejection result, or null if the transaction is allowed
   */
  private TransactionAddedResult checkLimits(
      final PendingTransaction incomingTx,
      final Map<Address, NavigableSet<Long>> incomingTxAuthorities,
      final Optional<Account> senderAccountAtChainHead) {
    final Address sender = incomingTx.getSender();
    final long incomingNonce = incomingTx.getNonce();
    final long senderNonceAtChainHead =
        senderAccountAtChainHead.map(AccountState::getNonce).orElse(0L);

    // case 1: the sender is already delegated, it can only have one transaction in the pool
    final boolean senderIsDelegated =
        senderAccountAtChainHead.map(DelegationLimits::isDelegated).orElse(false);
    if (senderIsDelegated && incomingNonce > senderNonceAtChainHead) {
      return rejectNonceTooFar(incomingTx, senderNonceAtChainHead, "the sender is delegated");
    }

    forgetDroppedDelegationsOf(sender);

    // case 2: someone else has a pending delegation for the sender, that is not delegated yet, so
    // it can also have the transaction after the next one
    if (pendingDelegations.hasSponsoredDelegation(sender, senderNonceAtChainHead)
        && incomingNonce > senderNonceAtChainHead + 1) {
      return rejectNonceTooFar(
          incomingTx,
          senderNonceAtChainHead + 1,
          "the sender has a pending delegation sent by another account");
    }

    // case 3: the sender is delegating itself, with a transaction that is pending in the pool, or
    // that is the incoming one. The sender nonce is incremented before the authorizations are
    // processed, so the first authorization that can be applied has the nonce after the one of the
    // delegation transaction, and applying it increments the account nonce again. So the nonces of
    // the authorizations that are applied are consumed by the delegation, and a transaction with
    // one of those nonces could never be executed, and the first one that can be executed after the
    // delegation has the next nonce, that is the last one allowed. An authorization with another
    // nonce is skipped, and it does not limit anything
    final List<PendingDelegations.SelfDelegation> selfDelegations =
        new ArrayList<>(pendingDelegations.pendingSelfDelegations(sender, senderNonceAtChainHead));
    Optional.ofNullable(incomingTxAuthorities.get(sender))
        .flatMap(tupleNonces -> PendingDelegations.selfDelegationOf(incomingNonce, tupleNonces))
        .ifPresent(selfDelegations::add);
    if (!selfDelegations.isEmpty()) {
      if (selfDelegations.stream().anyMatch(delegation -> delegation.consumes(incomingNonce))) {
        LOG.atTrace()
            .setMessage(
                "Drop transaction {} since its nonce is consumed by the pending delegation of the sender")
            .addArgument(incomingTx::toTraceLog)
            .log();
        return NONCE_ALREADY_CONSUMED_FOR_SENDER;
      }
      // the lowest delegation is the first one that is executed, so it is the one that limits
      final long highestAllowedNonce =
          selfDelegations.stream()
              .min(Comparator.comparingLong(PendingDelegations.SelfDelegation::firstConsumedNonce))
              .orElseThrow()
              .highestAllowedNonce();
      if (incomingNonce > highestAllowedNonce) {
        return rejectNonceTooFar(
            incomingTx, highestAllowedNonce, "the sender is delegating itself");
      }
    }

    // case 4: the incoming transaction is a delegation for other accounts that have pending
    // transactions that they would not be allowed to have once delegated
    return checkAuthoritiesOfIncomingDelegation(incomingTx, incomingTxAuthorities);
  }

  private static TransactionAddedResult rejectNonceTooFar(
      final PendingTransaction incomingTx, final long highestAllowedNonce, final String reason) {
    LOG.atTrace()
        .setMessage("Drop transaction {} since {} and only nonce up to {} is allowed")
        .addArgument(incomingTx::toTraceLog)
        .addArgument(reason)
        .addArgument(highestAllowedNonce)
        .log();
    return NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER;
  }

  /**
   * If the incoming transaction is a code delegation one, rejects it if any of its authorities has
   * already pending transactions in the pool that it would not be allowed to have once delegated,
   * otherwise it would be enough to send the delegation last to delegate an account with a full
   * queue. For an authority that is not the sender, the pending transactions can go up to its
   * current nonce + 1, while for the sender, that is delegating itself, they can not have a nonce
   * that is consumed by the delegation, and they can go up to the one after the last consumed.
   */
  private TransactionAddedResult checkAuthoritiesOfIncomingDelegation(
      final PendingTransaction incomingTx, final Map<Address, NavigableSet<Long>> authorities) {
    final Address sender = incomingTx.getSender();

    for (final var authorityAndTupleNonces : authorities.entrySet()) {
      final Address authority = authorityAndTupleNonces.getKey();
      final boolean authorityIsSender = authority.equals(sender);

      final SenderPendingTransactionsData authorityPendingData =
          pendingTransactions.getPendingTransactionsFor(authority);
      final List<PendingTransaction> authorityPendingTxs =
          authorityPendingData.pendingTransactions();
      // if it has no pending transactions there is nothing to check
      if (authorityPendingTxs.isEmpty()) {
        continue;
      }

      final boolean authorityHasDisallowedPendingTxs;
      if (authorityIsSender) {
        // self delegation, the nonces consumed by the authorizations that are applied are not
        // allowed, and the one after the last of them is the highest allowed
        authorityHasDisallowedPendingTxs =
            PendingDelegations.selfDelegationOf(
                    incomingTx.getNonce(), authorityAndTupleNonces.getValue())
                .map(
                    selfDelegation ->
                        authorityPendingTxs.stream()
                            .anyMatch(
                                pendingTx ->
                                    selfDelegation.consumes(pendingTx.getNonce())
                                        || pendingTx.getNonce()
                                            > selfDelegation.highestAllowedNonce()))
                .orElse(false);
      } else {
        // the nonce of the authority as the pool sees it. A tuple with a lower nonce can never be
        // applied, so it is ignored
        final long poolNonce = authorityPendingData.nonce();
        authorityHasDisallowedPendingTxs =
            !authorityAndTupleNonces.getValue().tailSet(poolNonce, true).isEmpty()
                && authorityPendingTxs.stream()
                    .anyMatch(pendingTx -> pendingTx.getNonce() > poolNonce + 1);
      }

      if (authorityHasDisallowedPendingTxs) {
        LOG.atTrace()
            .setMessage(
                "Drop transaction {} since it delegates {} that has pending transactions that it would not be allowed to have once delegated")
            .addArgument(incomingTx::toTraceLog)
            .addArgument(authority)
            .log();
        return DELEGATION_AUTHORITY_HAS_PENDING_TRANSACTIONS;
      }
    }
    return null;
  }

  private static boolean isDelegated(final Account account) {
    final var code = account.getCode();
    return code != null && hasCodeDelegation(code);
  }
}
