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
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public abstract class ParallelBlockTransactionProcessor {

  protected CompletableFuture<ParallelizedTransactionContext>[] futures;

  /**
   * Set once block processing no longer has any use for the speculative results.
   *
   * <p>Read from the executor threads, written from the block processing thread, hence volatile.
   */
  private volatile boolean abandoned;

  /**
   * Abandons the speculative executions dispatched by {@link #runAsyncBlock} that have not started
   * yet.
   *
   * <p>Block processing routinely stops before the last transaction: a transaction is invalid, the
   * block gas budget is exhausted, the state root does not match. Every result still queued at that
   * point is unusable, but the tasks stay runnable and keep every core busy long after the block
   * has been rejected. Since a block is rejected on attacker-supplied data, that turns one invalid
   * block into sustained CPU exhaustion.
   *
   * <p>Cancelling the futures would not help: {@link CompletableFuture#cancel} does not interrupt a
   * task that is already running and does not remove it from the executor queue. Tasks are
   * therefore made to check this flag and return early instead. Executions already in flight are
   * left to finish, so at most one transaction per executor thread still runs to completion.
   */
  public void abandonPendingExecutions() {
    abandoned = true;
  }

  protected CompletableFuture<ParallelizedTransactionContext> removeFuture(final int txIndex) {
    final CompletableFuture<ParallelizedTransactionContext> future = futures[txIndex];
    futures[txIndex] = null;
    return future;
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

    abandoned = false;
    futures = new CompletableFuture[transactions.size()];

    for (int i = 0; i < transactions.size(); i++) {
      final int txIndex = i;
      final Transaction transaction = transactions.get(i);

      futures[i] =
          CompletableFuture.supplyAsync(
              () ->
                  abandoned
                      // A null context means "no speculative result for this transaction", which
                      // getProcessingResult already handles by executing it on the block thread.
                      ? null
                      : runTransaction(
                          protocolContext,
                          blockHeader,
                          txIndex,
                          transaction,
                          miningBeneficiary,
                          blockHashLookup,
                          blobGasPrice,
                          blockAccessListBuilder,
                          maybeParentHeader),
              executor);
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
