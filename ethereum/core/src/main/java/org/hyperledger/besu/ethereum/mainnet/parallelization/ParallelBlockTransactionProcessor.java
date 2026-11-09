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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ParallelBlockTransactionProcessor {

  /**
   * Gas limit threshold above which a transaction is hoisted to the front of the parallel execution
   * queue so long-running transactions start warming and executing immediately.
   */
  public static final long HEAVY_TRANSACTION_GAS_THRESHOLD = 4_000_000L;

  protected CompletableFuture<ParallelizedTransactionContext>[] futures;

  /**
   * Tracks the highest transaction index already reached by the sequential block-import loop so
   * queued background tasks that have been overtaken can exit before loading world state.
   */
  protected final AtomicInteger currentSequentialTxIndex = new AtomicInteger(-1);

  protected CompletableFuture<ParallelizedTransactionContext> removeFuture(final int txIndex) {
    currentSequentialTxIndex.lazySet(txIndex);
    final CompletableFuture<ParallelizedTransactionContext> future = futures[txIndex];
    futures[txIndex] = null;
    return future;
  }

  protected void markInFlight(final int txIndex) {
    if (inFlight != null) {
      inFlight[txIndex] = true;
    }
  }

  protected boolean isInFlight(final int txIndex) {
    return inFlight != null && inFlight[txIndex];
  }

  protected volatile boolean[] inFlight;

  protected boolean isOvertaken(final int txIndex) {
    return txIndex <= currentSequentialTxIndex.get();
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
    inFlight = new boolean[size];
    currentSequentialTxIndex.set(-1);

    final int[] executionOrder = computeExecutionOrder(transactions);

    for (int i = 0; i < size; i++) {
      final int txIndex = executionOrder[i];
      final Transaction transaction = transactions.get(txIndex);

      futures[txIndex] =
          CompletableFuture.supplyAsync(
              () -> {
                if (isOvertaken(txIndex)) {
                  return null;
                }
                markInFlight(txIndex);
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

  /**
   * Orders transactions so that heavy transactions (gas limit &gt; {@link
   * #HEAVY_TRANSACTION_GAS_THRESHOLD}) are scheduled first (in descending gas limit order,
   * tie-broken by block index), followed by all remaining transactions in original block order.
   */
  static int[] computeExecutionOrder(final List<Transaction> transactions) {
    final int size = transactions.size();
    final int[] order = new int[size];
    int heavyCount = 0;
    for (int i = 0; i < size; i++) {
      if (transactions.get(i).getGasLimit() > HEAVY_TRANSACTION_GAS_THRESHOLD) {
        heavyCount++;
      }
    }
    if (heavyCount == 0) {
      for (int i = 0; i < size; i++) {
        order[i] = i;
      }
      return order;
    }

    final long[] heavyPacked = new long[heavyCount];
    int h = 0;
    int l = heavyCount;
    for (int i = 0; i < size; i++) {
      final long gasLimit = transactions.get(i).getGasLimit();
      if (gasLimit > HEAVY_TRANSACTION_GAS_THRESHOLD) {
        // Pack inverted gasLimit in high 42 bits and index in low 22 bits so ascending sort
        // orders by gasLimit DESC, then txIndex ASC.
        final long clampedGas = Math.min(gasLimit, (1L << 41) - 1L);
        heavyPacked[h++] = ((~clampedGas & ((1L << 42) - 1L)) << 22) | (i & 0x3FFFFFL);
      } else {
        order[l++] = i;
      }
    }
    Arrays.sort(heavyPacked);
    for (int i = 0; i < heavyCount; i++) {
      order[i] = (int) (heavyPacked[i] & 0x3FFFFFL);
    }
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
