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

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransactions;
import org.hyperledger.besu.ethereum.eth.transactions.SenderPendingTransactionsData;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAddedResult;
import org.hyperledger.besu.ethereum.eth.transactions.layered.LayeredPendingTransactions;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Pairs the layered pool with the frame transaction pool behind one {@link PendingTransactions}, so
 * the transaction pool, gossip and block building see a single pool. Frame transactions go to the
 * frame pool and everything else to the layered pool.
 *
 * <p>Block building is offered both pools' candidates in one pass, as the selector is not built to
 * run twice per block: frame transactions are merged into the layered pool's order by effective
 * priority fee, without reordering the layered candidates among themselves.
 */
public class CompositePendingTransactions implements PendingTransactions {

  private final LayeredPendingTransactions layered;
  private final FramePendingTransactions frames;
  private final Supplier<BlockHeader> chainHead;

  /**
   * Creates the composite.
   *
   * @param layered the pool for every non-frame transaction
   * @param frames the frame transaction pool
   * @param chainHead supplier of the chain head, for the base fee used to merge candidates
   */
  public CompositePendingTransactions(
      final LayeredPendingTransactions layered,
      final FramePendingTransactions frames,
      final Supplier<BlockHeader> chainHead) {
    this.layered = layered;
    this.frames = frames;
    this.chainHead = chainHead;
  }

  private PendingTransactions poolFor(final Transaction transaction) {
    return transaction.getType().supportsFrames() ? frames : layered;
  }

  @Override
  public boolean supportsFrameTransactions() {
    return true;
  }

  @Override
  public TransactionAddedResult addTransaction(
      final PendingTransaction transaction, final Optional<Account> maybeSenderAccount) {
    return poolFor(transaction.getTransaction()).addTransaction(transaction, maybeSenderAccount);
  }

  @Override
  public void selectTransactions(final PendingTransactionsSelector selector) {
    final List<PendingTransaction> layeredCandidates = layered.selectionCandidates();
    final List<PendingTransaction> frameCandidates = frames.selectionCandidates();
    final Map<PendingTransaction, TransactionSelectionResult> results =
        selector.evaluatePendingTransactions(
            List.copyOf(merge(layeredCandidates, frameCandidates)));

    final Map<PendingTransaction, TransactionSelectionResult> layeredResults = new HashMap<>();
    final Map<PendingTransaction, TransactionSelectionResult> frameResults = new HashMap<>();
    results.forEach(
        (pending, result) ->
            (pending.getTransaction().getType().supportsFrames() ? frameResults : layeredResults)
                .put(pending, result));
    layered.applySelectionResults(layeredResults);
    frames.applySelectionResults(frameResults);
  }

  private List<PendingTransaction> merge(
      final List<PendingTransaction> layeredCandidates,
      final List<PendingTransaction> frameCandidates) {
    if (frameCandidates.isEmpty()) {
      return layeredCandidates;
    }
    final Optional<Wei> baseFee = chainHead.get().getBaseFee();
    final List<PendingTransaction> merged =
        new ArrayList<>(layeredCandidates.size() + frameCandidates.size());
    int next = 0;
    for (final PendingTransaction candidate : layeredCandidates) {
      final Wei fee = candidate.getTransaction().getEffectivePriorityFeePerGas(baseFee);
      while (next < frameCandidates.size()
          && frameCandidates
              .get(next)
              .getTransaction()
              .getEffectivePriorityFeePerGas(baseFee)
              .greaterThan(fee)) {
        merged.add(frameCandidates.get(next++));
      }
      merged.add(candidate);
    }
    merged.addAll(frameCandidates.subList(next, frameCandidates.size()));
    return merged;
  }

  @Override
  public void manageBlockAdded(
      final BlockHeader blockHeader,
      final List<Transaction> confirmedTransactions,
      final List<Transaction> reorgTransactions,
      final FeeMarket feeMarket) {
    layered.manageBlockAdded(
        blockHeader,
        confirmedTransactions.stream().filter(t -> !t.getType().supportsFrames()).toList(),
        reorgTransactions.stream().filter(t -> !t.getType().supportsFrames()).toList(),
        feeMarket);
    frames.manageBlockAdded(
        blockHeader,
        confirmedTransactions.stream().filter(t -> t.getType().supportsFrames()).toList(),
        reorgTransactions.stream().filter(t -> t.getType().supportsFrames()).toList(),
        feeMarket);
  }

  @Override
  public void reset() {
    layered.reset();
    frames.reset();
  }

  @Override
  public void evictOldTransactions() {
    layered.evictOldTransactions();
    frames.evictOldTransactions();
  }

  @Override
  public List<Transaction> getLocalTransactions() {
    return concat(layered.getLocalTransactions(), frames.getLocalTransactions());
  }

  @Override
  public List<Transaction> getPriorityTransactions() {
    return concat(layered.getPriorityTransactions(), frames.getPriorityTransactions());
  }

  @Override
  public long maxSize() {
    return layered.maxSize();
  }

  @Override
  public int size() {
    return layered.size() + frames.size();
  }

  @Override
  public boolean containsTransaction(final Transaction transaction) {
    return poolFor(transaction).containsTransaction(transaction);
  }

  @Override
  public Optional<Transaction> getTransactionByHash(final Hash transactionHash) {
    return layered
        .getTransactionByHash(transactionHash)
        .or(() -> frames.getTransactionByHash(transactionHash));
  }

  @Override
  public Collection<PendingTransaction> getPendingTransactions() {
    return concat(
        List.copyOf(layered.getPendingTransactions()),
        List.copyOf(frames.getPendingTransactions()));
  }

  @Override
  public SenderPendingTransactionsData getPendingTransactionsFor(final Address sender) {
    final SenderPendingTransactionsData fromLayered = layered.getPendingTransactionsFor(sender);
    final SenderPendingTransactionsData fromFrames = frames.getPendingTransactionsFor(sender);
    if (fromFrames.pendingTransactions().isEmpty()) {
      return fromLayered;
    }
    if (fromLayered.pendingTransactions().isEmpty()) {
      return fromFrames;
    }
    return new SenderPendingTransactionsData(
        sender,
        fromLayered.nonce(),
        concat(fromLayered.pendingTransactions(), fromFrames.pendingTransactions()));
  }

  @Override
  public Map<Address, SenderPendingTransactionsData> getPendingTransactionsBySender() {
    final Map<Address, SenderPendingTransactionsData> result =
        new LinkedHashMap<>(layered.getPendingTransactionsBySender());
    frames
        .getPendingTransactionsBySender()
        .keySet()
        .forEach(sender -> result.put(sender, getPendingTransactionsFor(sender)));
    return result;
  }

  @Override
  public OptionalLong getNextNonceForSender(final Address sender) {
    return layered.getNextNonceForSender(sender);
  }

  @Override
  public String toTraceLog() {
    return layered.toTraceLog() + " | " + frames.toTraceLog();
  }

  @Override
  public String logStats() {
    return layered.logStats() + " | " + frames.logStats();
  }

  @Override
  public Status getStatus() {
    final Status layeredStatus = layered.getStatus();
    return new Status(layeredStatus.pendingCount() + frames.size(), layeredStatus.queuedCount());
  }

  @Override
  public Optional<Transaction> restoreBlob(final Transaction transaction) {
    return poolFor(transaction).restoreBlob(transaction);
  }

  private static <T> List<T> concat(final List<? extends T> a, final List<? extends T> b) {
    final List<T> joined = new ArrayList<>(a.size() + b.size());
    joined.addAll(a);
    joined.addAll(b);
    return joined;
  }
}
