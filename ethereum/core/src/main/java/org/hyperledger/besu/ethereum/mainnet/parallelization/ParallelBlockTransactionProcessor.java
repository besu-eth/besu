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
import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class ParallelBlockTransactionProcessor {

  /**
   * Gas limit above which a transaction is heavy: heavy transactions start first, and the
   * optimistic processor waits for one that a worker is already executing instead of executing it
   * again.
   */
  static final long HEAVY_TRANSACTION_GAS_THRESHOLD = 4_000_000L;

  static boolean isHeavy(final Transaction transaction) {
    return transaction.getGasLimit() > HEAVY_TRANSACTION_GAS_THRESHOLD;
  }

  protected CompletableFuture<ParallelizedTransactionContext>[] futures;

  /** Set to 1 by a worker once it starts executing the transaction at that index. */
  private AtomicIntegerArray inFlight;

  protected CompletableFuture<ParallelizedTransactionContext> removeFuture(final int txIndex) {
    final CompletableFuture<ParallelizedTransactionContext> future = futures[txIndex];
    futures[txIndex] = null;
    return future;
  }

  /**
   * Whether a worker has started executing the transaction at this index. Its future is still
   * running when the transaction has started but not finished, and also when it is still queued.
   */
  protected boolean isInFlight(final int txIndex) {
    return inFlight.get(txIndex) != 0;
  }

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

    final int size = transactions.size();
    futures = new CompletableFuture[size];
    final AtomicIntegerArray started = new AtomicIntegerArray(size);
    inFlight = started;

    final int[] executionOrder = computeExecutionOrder(transactions);

    for (int i = 0; i < size; i++) {
      final int txIndex = executionOrder[i];
      final Transaction transaction = transactions.get(txIndex);

      futures[txIndex] =
          CompletableFuture.supplyAsync(
              () -> {
                started.set(txIndex, 1);
                return runTransaction(
                    protocolContext,
                    blockHeader,
                    txIndex,
                    transaction,
                    miningBeneficiary,
                    blockHashLookup,
                    blobGasPrice,
                    blockAccessListBuilder,
                    maybeParentHeader);
              },
              executor);
    }
  }

  /** Orders the heavy transactions first, then the others, each group in block order. */
  static int[] computeExecutionOrder(final List<Transaction> transactions) {
    final int size = transactions.size();
    final int[] order = new int[size];
    final int[] light = new int[size];
    int heavyCount = 0;
    int lightCount = 0;
    for (int i = 0; i < size; i++) {
      if (isHeavy(transactions.get(i))) {
        order[heavyCount++] = i;
      } else {
        light[lightCount++] = i;
      }
    }
    System.arraycopy(light, 0, order, heavyCount, lightCount);
    return order;
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

  public abstract Optional<TransactionProcessingResult> getProcessingResult(
      MutableWorldState worldState,
      Address miningBeneficiary,
      Transaction transaction,
      int location,
      Optional<Counter> confirmedParallelizedTransactionCounter,
      Optional<Counter> conflictingButCachedTransactionCounter);
}
