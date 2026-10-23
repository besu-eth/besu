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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import static org.hyperledger.besu.ethereum.eth.transactions.frame.FrameRemovalReason.CONFIRMED;
import static org.hyperledger.besu.ethereum.eth.transactions.frame.FrameRemovalReason.EVICTED;
import static org.hyperledger.besu.ethereum.eth.transactions.frame.FrameRemovalReason.INVALIDATED;
import static org.hyperledger.besu.ethereum.eth.transactions.frame.FrameRemovalReason.REPLACED;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.RemovalReason;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionEventNotifier;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.math.BigInteger;
import java.time.Clock;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The EIP-8141 public mempool for frame transactions, kept apart from the layered pool because its
 * rules do not fit nonce sequencing: a frame transaction's validity rests on simulating its
 * validation prefix, its nonce may be keyed (EIP-8250), and a third party may pay for it.
 *
 * <ul>
 *   <li>At most one pending transaction per sender. A transaction with the same nonce identity (key
 *       set and sequence) replaces it only by raising both fee caps by the configured price bump.
 *   <li>The summed maximum cost of a payer's pending transactions never exceeds its balance at the
 *       head, and a payer running code has at most one pending transaction.
 *   <li>Every new head drops included and expired transactions and re-simulates the rest.
 *   <li>When full, the transaction with the nearest expiry, then the lowest priority fee, makes
 *       room, provided the newcomer pays more.
 * </ul>
 */
public class FramePendingTransactions implements PendingTransactions {

  private static final Logger LOG = LoggerFactory.getLogger(FramePendingTransactions.class);

  /** Pending frame transactions held at once. */
  public static final int DEFAULT_MAX_SIZE = 1024;

  /** Consecutive penalized block-building attempts before a transaction is evicted. */
  static final int MAX_UNPAID_BUILD_ATTEMPTS = 3;

  /** Pending non-canonical paymaster transactions allowed per paymaster. */
  static final int MAX_PENDING_TXS_USING_NON_CANONICAL_PAYMASTER = 1;

  private final TransactionPoolConfiguration configuration;
  private final FrameTransactionAdmission admission;
  private final Supplier<BlockHeader> chainHead;
  private final Clock clock;
  private final int maxSize;
  private final TransactionEventNotifier.Emitter eventEmitter;

  private final Map<Address, Entry> bySender = new HashMap<>();
  private final Map<Hash, Entry> byHash = new HashMap<>();
  private final Map<Address, Wei> reservedByPayer = new HashMap<>();
  private final Map<Address, Integer> pendingByCodedPaymaster = new HashMap<>();

  private static final class Entry {
    final PendingTransaction pending;
    FrameTransactionAdmission.Admitted admitted;
    int unpaidBuildAttempts;

    Entry(final PendingTransaction pending, final FrameTransactionAdmission.Admitted admitted) {
      this.pending = pending;
      this.admitted = admitted;
    }

    Transaction transaction() {
      return pending.getTransaction();
    }

    OptionalLong deadline() {
      return admitted.prefix().expiryDeadline();
    }
  }

  /**
   * Creates the pool.
   *
   * @param configuration the transaction pool configuration
   * @param admission the admission check
   * @param chainHead supplier of the chain head
   * @param clock the clock used for retention
   * @param maxSize the most pending transactions held at once
   * @param eventEmitter notifies listeners of added and dropped transactions
   */
  public FramePendingTransactions(
      final TransactionPoolConfiguration configuration,
      final FrameTransactionAdmission admission,
      final Supplier<BlockHeader> chainHead,
      final Clock clock,
      final int maxSize,
      final TransactionEventNotifier.Emitter eventEmitter) {
    this.configuration = configuration;
    this.admission = admission;
    this.chainHead = chainHead;
    this.clock = clock;
    this.maxSize = maxSize;
    this.eventEmitter = eventEmitter;
  }

  @Override
  public boolean supportsFrameTransactions() {
    return true;
  }

  @Override
  public synchronized TransactionAddedResult addTransaction(
      final PendingTransaction pendingTransaction, final Optional<Account> maybeSenderAccount) {
    final Transaction transaction = pendingTransaction.getTransaction();
    if (byHash.containsKey(transaction.getHash())) {
      return TransactionAddedResult.ALREADY_KNOWN;
    }

    final FrameTransactionAdmission.Result result = admission.admit(transaction, chainHead.get());
    if (!result.isAdmitted()) {
      LOG.atDebug()
          .setMessage("Frame transaction {} not admitted: {} {}")
          .addArgument(transaction::getHash)
          .addArgument(result.rejection())
          .addArgument(result.message())
          .log();
      return TransactionAddedResult.rejected(result.rejection());
    }
    final FrameTransactionAdmission.Admitted admitted = result.admitted().get();

    final Entry existing = bySender.get(transaction.getSender());
    if (existing != null) {
      if (!sameNonceIdentity(existing.transaction(), transaction)) {
        return TransactionAddedResult.rejected(
            TransactionInvalidReason.FRAME_SENDER_ALREADY_PENDING);
      }
      if (!outbids(transaction, existing.transaction())) {
        return TransactionAddedResult.REJECTED_UNDERPRICED_REPLACEMENT;
      }
    }

    final Wei alreadyReserved =
        reservedByPayer
            .getOrDefault(admitted.payer(), Wei.ZERO)
            .subtract(
                existing != null && existing.admitted.payer().equals(admitted.payer())
                    ? existing.admitted.maxCost()
                    : Wei.ZERO);
    if (alreadyReserved.add(admitted.maxCost()).greaterThan(admitted.payerBalance())) {
      return TransactionAddedResult.rejected(
          TransactionInvalidReason.FRAME_PAYER_EXPOSURE_EXCEEDED);
    }
    if (admitted.codedPaymaster()) {
      final int pending =
          pendingByCodedPaymaster.getOrDefault(admitted.payer(), 0)
              - (existing != null && existing.admitted.payer().equals(admitted.payer()) ? 1 : 0);
      if (pending >= MAX_PENDING_TXS_USING_NON_CANONICAL_PAYMASTER) {
        return TransactionAddedResult.rejected(
            TransactionInvalidReason.FRAME_PAYMASTER_PENDING_LIMIT);
      }
    }

    if (existing == null && byHash.size() >= maxSize && !makeRoomFor(transaction)) {
      return TransactionAddedResult.rejected(TransactionInvalidReason.TRANSACTION_PRICE_TOO_LOW);
    }

    if (existing != null) {
      remove(existing, REPLACED);
    }
    final Entry entry = new Entry(pendingTransaction, admitted);
    insert(entry);
    eventEmitter.notifyPendingTransactionAdded(pendingTransaction);
    return existing != null
        ? TransactionAddedResult.createForReplacement(existing.pending)
        : TransactionAddedResult.ADDED;
  }

  @Override
  public void selectTransactions(final PendingTransactionsSelector selector) {
    final List<PendingTransaction> candidates = selectionCandidates();
    applySelectionResults(selector.evaluatePendingTransactions(List.copyOf(candidates)));
  }

  /**
   * The pending transactions offered to block building, highest effective priority fee first.
   *
   * @return the candidates
   */
  public synchronized List<PendingTransaction> selectionCandidates() {
    final Optional<Wei> baseFee = chainHead.get().getBaseFee();
    return byHash.values().stream()
        .sorted(
            Comparator.comparing(
                    (Entry e) -> e.transaction().getEffectivePriorityFeePerGas(baseFee))
                .reversed())
        .map(e -> e.pending)
        .toList();
  }

  /**
   * Applies block-building results: discarded transactions are dropped, and a transaction penalized
   * on too many consecutive attempts is evicted, bounding the unpaid validation work it can cost.
   *
   * @param results the selection results for this pool's candidates
   */
  public synchronized void applySelectionResults(
      final Map<PendingTransaction, TransactionSelectionResult> results) {
    for (final var result : results.entrySet()) {
      final Entry entry = byHash.get(result.getKey().getHash());
      if (entry == null) {
        continue;
      }
      if (result.getValue().discard()) {
        remove(entry, INVALIDATED);
      } else if (result.getValue().penalize()) {
        if (++entry.unpaidBuildAttempts >= MAX_UNPAID_BUILD_ATTEMPTS) {
          remove(entry, INVALIDATED);
        }
      } else {
        entry.unpaidBuildAttempts = 0;
      }
    }
  }

  @Override
  public synchronized void manageBlockAdded(
      final BlockHeader blockHeader,
      final List<Transaction> confirmedTransactions,
      final List<Transaction> reorgTransactions,
      final FeeMarket feeMarket) {
    for (final Transaction confirmed : confirmedTransactions) {
      final Entry entry = byHash.get(confirmed.getHash());
      if (entry != null) {
        remove(entry, CONFIRMED);
      }
    }
    // Re-simulate everything still pending against the new head. The EIP recommends indexing
    // pending transactions by the state their prefix read and revalidating only those a block
    // touched; re-simulating all of them is equivalent, and bounded by the pool size.
    for (final Entry entry : List.copyOf(byHash.values())) {
      final FrameTransactionAdmission.Result result =
          admission.admit(entry.transaction(), blockHeader);
      if (result.isAdmitted()) {
        unreserve(entry);
        entry.admitted = result.admitted().get();
        reserve(entry);
      } else {
        LOG.atTrace()
            .setMessage("Frame transaction {} no longer admissible: {} {}")
            .addArgument(entry.transaction()::getHash)
            .addArgument(result.rejection())
            .addArgument(result.message())
            .log();
        remove(entry, INVALIDATED);
      }
    }
    // A balance that fell can leave a payer over-reserved: drop its lowest-paying transactions.
    for (final Address payer : List.copyOf(reservedByPayer.keySet())) {
      final List<Entry> paid =
          byHash.values().stream()
              .filter(e -> e.admitted.payer().equals(payer))
              .sorted(Comparator.comparing((Entry e) -> e.transaction().getMaxGasPrice()))
              .toList();
      for (final Entry entry : paid) {
        if (!reservedByPayer
            .getOrDefault(payer, Wei.ZERO)
            .greaterThan(entry.admitted.payerBalance())) {
          break;
        }
        remove(entry, INVALIDATED);
      }
    }
  }

  @Override
  public synchronized void evictOldTransactions() {
    final long cutoff =
        clock.millis() - configuration.getPendingTxRetentionPeriod() * 60L * 60L * 1000L;
    for (final Entry entry : List.copyOf(byHash.values())) {
      if (entry.pending.getAddedAt() < cutoff) {
        remove(entry, EVICTED);
      }
    }
  }

  @Override
  public synchronized void reset() {
    for (final Entry entry : List.copyOf(byHash.values())) {
      remove(entry, EVICTED);
    }
  }

  @Override
  public synchronized List<Transaction> getLocalTransactions() {
    return byHash.values().stream()
        .filter(e -> e.pending.isReceivedFromLocalSource())
        .map(Entry::transaction)
        .toList();
  }

  @Override
  public synchronized List<Transaction> getPriorityTransactions() {
    return byHash.values().stream()
        .filter(e -> e.pending.hasPriority())
        .map(Entry::transaction)
        .toList();
  }

  @Override
  public long maxSize() {
    return maxSize;
  }

  @Override
  public synchronized int size() {
    return byHash.size();
  }

  @Override
  public synchronized boolean containsTransaction(final Transaction transaction) {
    return byHash.containsKey(transaction.getHash());
  }

  @Override
  public synchronized Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    return Optional.ofNullable(byHash.get(transactionHash)).map(Entry::transaction);
  }

  @Override
  public synchronized Collection<PendingTransaction> getPendingTransactions() {
    return byHash.values().stream().map(e -> e.pending).toList();
  }

  @Override
  public synchronized SenderPendingTransactionsData getPendingTransactionsFor(
      final Address sender) {
    final Entry entry = bySender.get(sender);
    return entry == null
        ? SenderPendingTransactionsData.empty(sender)
        : new SenderPendingTransactionsData(
            sender, entry.transaction().getNonce(), List.of(entry.pending));
  }

  @Override
  public synchronized Map<Address, SenderPendingTransactionsData> getPendingTransactionsBySender() {
    final Map<Address, SenderPendingTransactionsData> result = new LinkedHashMap<>();
    bySender.keySet().forEach(sender -> result.put(sender, getPendingTransactionsFor(sender)));
    return result;
  }

  @Override
  public OptionalLong getNextNonceForSender(final Address sender) {
    // A frame transaction's next nonce depends on its key set, which the pool cannot predict.
    return OptionalLong.empty();
  }

  @Override
  public synchronized String toTraceLog() {
    return "Frame transactions " + byHash.keySet() + ", reserved " + reservedByPayer;
  }

  @Override
  public synchronized String logStats() {
    return "Frame " + byHash.size() + "/" + maxSize;
  }

  @Override
  public synchronized Status getStatus() {
    return new Status(byHash.size(), 0);
  }

  @Override
  public Optional<Transaction> restoreBlob(final Transaction transaction) {
    return Optional.empty();
  }

  private boolean makeRoomFor(final Transaction incoming) {
    final Optional<Wei> baseFee = chainHead.get().getBaseFee();
    final Comparator<Entry> evictionOrder =
        Comparator.comparing((Entry e) -> e.deadline().isPresent() ? 0 : 1) // expiring first
            .thenComparing(
                e -> e.deadline().orElse(Long.MAX_VALUE), Long::compareUnsigned) // nearest first
            .thenComparing(e -> e.transaction().getEffectivePriorityFeePerGas(baseFee));
    final Optional<Entry> victim = byHash.values().stream().min(evictionOrder);
    if (victim.isEmpty()
        || !incoming
            .getEffectivePriorityFeePerGas(baseFee)
            .greaterThan(victim.get().transaction().getEffectivePriorityFeePerGas(baseFee))) {
      return false;
    }
    remove(victim.get(), EVICTED);
    return true;
  }

  private void insert(final Entry entry) {
    bySender.put(entry.transaction().getSender(), entry);
    byHash.put(entry.transaction().getHash(), entry);
    reserve(entry);
  }

  private void remove(final Entry entry, final RemovalReason reason) {
    if (byHash.remove(entry.transaction().getHash()) == null) {
      return;
    }
    bySender.remove(entry.transaction().getSender(), entry);
    unreserve(entry);
    eventEmitter.notifyPendingTransactionDropped(entry.pending, reason);
  }

  private void reserve(final Entry entry) {
    final Address payer = entry.admitted.payer();
    reservedByPayer.merge(payer, entry.admitted.maxCost(), Wei::add);
    if (entry.admitted.codedPaymaster()) {
      pendingByCodedPaymaster.merge(payer, 1, Integer::sum);
    }
  }

  private void unreserve(final Entry entry) {
    final Address payer = entry.admitted.payer();
    final Wei remaining =
        reservedByPayer.getOrDefault(payer, Wei.ZERO).subtract(entry.admitted.maxCost());
    if (remaining.isZero()) {
      reservedByPayer.remove(payer);
    } else {
      reservedByPayer.put(payer, remaining);
    }
    if (entry.admitted.codedPaymaster()) {
      pendingByCodedPaymaster.computeIfPresent(payer, (p, n) -> n > 1 ? n - 1 : null);
    }
  }

  /** Two frame transactions are alternatives when they consume the same nonce set at once. */
  private static boolean sameNonceIdentity(final Transaction a, final Transaction b) {
    return a.getNonce() == b.getNonce() && Objects.equals(a.getNonceKeys(), b.getNonceKeys());
  }

  private boolean outbids(final Transaction replacement, final Transaction existing) {
    return bumped(replacement.getMaxFeePerGas(), existing.getMaxFeePerGas())
        && bumped(replacement.getMaxPriorityFeePerGas(), existing.getMaxPriorityFeePerGas());
  }

  private boolean bumped(final Optional<Wei> replacement, final Optional<Wei> existing) {
    final BigInteger old = existing.orElse(Wei.ZERO).getAsBigInteger();
    final BigInteger required =
        old.multiply(BigInteger.valueOf(100L + configuration.getPriceBump().getValue()))
            .add(BigInteger.valueOf(99))
            .divide(BigInteger.valueOf(100));
    return replacement.orElse(Wei.ZERO).getAsBigInteger().compareTo(required) >= 0;
  }
}
