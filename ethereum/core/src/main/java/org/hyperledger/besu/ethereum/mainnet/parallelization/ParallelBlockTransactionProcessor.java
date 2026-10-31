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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ParallelBlockTransactionProcessor {

  protected CompletableFuture<ParallelizedTransactionContext>[] futures;

  protected CompletableFuture<ParallelizedTransactionContext> removeFuture(final int txIndex) {
    final CompletableFuture<ParallelizedTransactionContext> future = futures[txIndex];
    futures[txIndex] = null;
    return future;
  }

  /**
   * Runs the block's transactions on the executor, completing the future of each with its result.
   * Rather than a task per transaction, which the threads of a pool would take from its queue one
   * at a time, contending for the queue's lock with transactions that run in a few microseconds,
   * one task per processor takes the next transaction from a shared counter, so the transactions
   * still start in block order.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void runAsyncBlock(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final List<Transaction> transactions,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Executor executor,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {

    final int transactionCount = transactions.size();
    futures = new CompletableFuture[transactionCount];
    for (int i = 0; i < transactionCount; i++) {
      futures[i] = new CompletableFuture<>();
    }
    // the slots of futures are cleared as the results are consumed
    final CompletableFuture<ParallelizedTransactionContext>[] results = futures.clone();
    final AtomicInteger nextTransaction = new AtomicInteger();
    final Runnable runner =
        () -> {
          int txIndex;
          while ((txIndex = nextTransaction.getAndIncrement()) < transactionCount) {
            final CompletableFuture<ParallelizedTransactionContext> result = results[txIndex];
            if (result.isDone()) {
              continue; // cancelled
            }
            try {
              result.complete(
                  runTransaction(
                      protocolContext,
                      blockHeader,
                      txIndex,
                      transactions.get(txIndex),
                      miningBeneficiary,
                      blockHashLookup,
                      blobGasPrice,
                      blockAccessListBuilder,
                      maybeParentHeader));
            } catch (final Throwable t) {
              result.completeExceptionally(t);
            }
          }
        };
    final int runners = Math.min(transactionCount, Runtime.getRuntime().availableProcessors());
    for (int i = 0; i < runners; i++) {
      executor.execute(runner);
    }
  }

  /**
   * Cancels every still-pending speculative future and nulls its slot. Already-consumed slots are
   * null, so on the success path this is a no-op.
   */
  public void abort() {
    if (futures == null) {
      return;
    }
    for (int i = 0; i < futures.length; i++) {
      final CompletableFuture<ParallelizedTransactionContext> future = futures[i];
      if (future != null) {
        future.cancel(true);
        futures[i] = null;
      }
    }
  }

  /** World state at the parent block. Call only when the parent header is known to be present. */
  protected Optional<BonsaiWorldState> getWorldState(
      final ProtocolContext protocolContext, final BlockHeader parentHeader) {
    return protocolContext
        .getWorldStateArchive()
        .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parentHeader))
        .map(BonsaiWorldState.class::cast);
  }

  /**
   * Copies the transaction for a worker after recovering its sender on the block's own transaction,
   * which newPayload's sender precompute also uses, so each sender is recovered once.
   */
  protected static Transaction detachedCopyWithSender(final Transaction transaction) {
    try {
      transaction.getSender();
    } catch (final IllegalArgumentException | IllegalStateException e) {
      // The copy's validation reports a signature that does not recover a sender
    }
    return transaction.detachedCopy();
  }

  protected abstract ParallelizedTransactionContext runTransaction(
      ProtocolContext protocolContext,
      BlockHeader blockHeader,
      int transactionLocation,
      Transaction transaction,
      Address miningBeneficiary,
      BlockHashLookup blockHashLookup,
      Wei blobGasPrice,
      Optional<BlockAccessListBuilder> blockAccessListBuilder,
      Optional<BlockHeader> maybeParentHeader);

  /**
   * Imports into the block's world state the state changes of results returned by {@link
   * #getProcessingResult} but not imported yet, before anything else reads or changes that state.
   * Nothing is pending by default: the results are imported as they are returned.
   *
   * @param worldState the block's world state
   * @param executor runs the import, possibly in several tasks
   */
  public void importPendingStateChanges(
      final MutableWorldState worldState, final Executor executor) {}

  public abstract Optional<TransactionProcessingResult> getProcessingResult(
      MutableWorldState worldState,
      Address miningBeneficiary,
      Transaction transaction,
      int location,
      Optional<Counter> confirmedParallelizedTransactionCounter,
      Optional<Counter> conflictingButCachedTransactionCounter);
}
