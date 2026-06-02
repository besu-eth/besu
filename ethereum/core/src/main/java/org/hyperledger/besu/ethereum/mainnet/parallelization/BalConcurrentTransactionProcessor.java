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
package org.hyperledger.besu.ethereum.mainnet.parallelization;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessListAddressView;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.mainnet.parallelization.prefetch.BalPrefetcher;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.BonsaiWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.provider.WorldStateQueryParams;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.PathBasedWorldState;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"unchecked", "rawtypes"})
public class BalConcurrentTransactionProcessor extends ParallelBlockTransactionProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(BalConcurrentTransactionProcessor.class);

  private final MainnetTransactionProcessor transactionProcessor;
  private final BlockAccessList blockAccessList;
  private final BlockAccessListAddressView blockAccessListAddressView;
  private final Duration balProcessingTimeout;
  private final Optional<BalPrefetcher> maybePrefetcher;

  public BalConcurrentTransactionProcessor(
      final MainnetTransactionProcessor transactionProcessor,
      final BlockAccessList blockAccessList,
      final BalConfiguration balConfiguration) {
    this.transactionProcessor = transactionProcessor;
    this.blockAccessList = blockAccessList;
    this.blockAccessListAddressView = BlockAccessListAddressView.of(blockAccessList);
    this.balProcessingTimeout = balConfiguration.getBalProcessingTimeout();
    this.maybePrefetcher =
        balConfiguration.isBalPreFetchReadingEnabled()
            ? Optional.of(
                new BalPrefetcher(
                    balConfiguration.isBalPreFetchSortingEnabled(),
                    balConfiguration.getBalPreFetchBatchSize()))
            : Optional.empty();
  }

  private Optional<BonsaiWorldState> getWorldStateForTransaction(
      final ProtocolContext protocolContext,
      final Optional<BlockHeader> maybeParentHeader,
      final int transactionLocation) {
    return maybeParentHeader.flatMap(
        blockHeader ->
            protocolContext
                .getWorldStateArchive()
                .getWorldState(
                    WorldStateQueryParams.newBuilder()
                        .withBlockHeader(blockHeader)
                        .withShouldWorldStateUpdateHead(false)
                        .withBalOverlay(blockAccessListAddressView, (long) transactionLocation + 1L)
                        .build())
                .map(BonsaiWorldState.class::cast));
  }

  @Override
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

    maybePrefetcher.ifPresent(
        balPrefetchMechanism -> {
          final Optional<BonsaiWorldState> maybeWorldState =
              getWorldState(protocolContext, maybeParentHeader);
          if (maybeWorldState.isPresent()) {
            balPrefetchMechanism
                .prefetch(maybeWorldState.get(), blockAccessList, executor)
                .exceptionally(
                    ex -> {
                      LOG.error("Prefetch failed", ex);
                      return null;
                    })
                .whenComplete((result, ex) -> maybeWorldState.get().close());
          } else {
            LOG.info("Prefetcher block header for block not loaded {}", blockHeader);
          }
        });
    super.runAsyncBlock(
        protocolContext,
        blockHeader,
        transactions,
        miningBeneficiary,
        blockHashLookup,
        blobGasPrice,
        executor,
        blockAccessListBuilder,
        maybeParentHeader);
  }

  @Override
  protected ParallelizedTransactionContext runTransaction(
      final ProtocolContext protocolContext,
      final BlockHeader blockHeader,
      final int transactionLocation,
      final Transaction transaction,
      final Address miningBeneficiary,
      final BlockHashLookup blockHashLookup,
      final Wei blobGasPrice,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder,
      final Optional<BlockHeader> maybeParentHeader) {

    final BonsaiWorldState ws =
        getWorldStateForTransaction(protocolContext, maybeParentHeader, transactionLocation)
            .orElse(null);
    if (ws == null) {
      return null;
    }

    try {
      ws.disableCacheMerkleTrieLoader();
      final ParallelizedTransactionContext.Builder ctxBuilder =
          new ParallelizedTransactionContext.Builder();

      final PathBasedWorldStateUpdateAccumulator<?> blockUpdater = ws.updater();
      final WorldUpdater txUpdater = blockUpdater.updater();
      final Optional<AccessLocationTracker> txTracker =
          blockAccessListBuilder.map(
              b ->
                  BlockAccessListBuilder.createTransactionAccessLocationTracker(
                      transactionLocation));

      final TransactionProcessingResult result =
          transactionProcessor.processTransaction(
              txUpdater,
              blockHeader,
              transaction.detachedCopy(),
              miningBeneficiary,
              OperationTracer.NO_TRACING,
              blockHashLookup,
              TransactionValidationParams.processingBlock(),
              blobGasPrice,
              txTracker);

      ctxBuilder.transactionProcessingResult(result);

      return ctxBuilder.build();
    } finally {
      ws.close();
    }
  }

  @Override
  // TODO: Throw instead of returning Optional.empty()?
  public Optional<TransactionProcessingResult> getProcessingResult(
      final MutableWorldState worldState,
      final Address miningBeneficiary,
      final Transaction transaction,
      final int txIndex,
      final Optional<Counter> confirmedParallelizedTransactionCounter,
      final Optional<Counter> conflictingButCachedTransactionCounter) {

    final CompletableFuture<ParallelizedTransactionContext> future = removeFuture(txIndex);
    if (future != null) {
      try {
        final ParallelizedTransactionContext ctx =
            balProcessingTimeout.isNegative()
                ? future.join()
                : future.get(balProcessingTimeout.toNanos(), TimeUnit.NANOSECONDS);

        if (ctx == null) {
          LOG.trace("Transaction context for transaction {} is empty.", txIndex);
          return Optional.empty();
        }

        final PathBasedWorldState pathWs = (PathBasedWorldState) worldState;
        final PathBasedWorldStateUpdateAccumulator blockAccumulator =
            (PathBasedWorldStateUpdateAccumulator) pathWs.updater();

        final TransactionProcessingResult result = ctx.transactionProcessingResult();
        final Optional<PartialBlockAccessView> maybePartialBlockAccessView =
            result.getPartialBlockAccessView();
        if (maybePartialBlockAccessView.isEmpty()) {
          LOG.trace("Partial block access view for transaction {} is empty.", txIndex);
          return Optional.empty();
        }

        applyWritesFromPartialBlockAccessView(maybePartialBlockAccessView.get(), blockAccumulator);

        confirmedParallelizedTransactionCounter.ifPresent(Counter::inc);
        result.setIsProcessedInParallel(Optional.of(Boolean.TRUE));

        return Optional.of(result);
      } catch (final TimeoutException e) {
        future.cancel(true);
        LOG.error(
            "Timed out waiting {}ms for transaction {} processing result.",
            balProcessingTimeout.toMillis(),
            txIndex);
        return Optional.empty();
      } catch (final InterruptedException e) {
        future.cancel(true);
        Thread.currentThread().interrupt();
        LOG.error("Interrupted while waiting for transaction {} processing result.", txIndex, e);
        return Optional.empty();
      } catch (final Exception e) {
        LOG.error(
            "Error integrating transaction processing result for transaction {}.", txIndex, e);
        return Optional.empty();
      }
    }

    LOG.error("No future found for transaction {}.", txIndex);
    return Optional.empty();
  }

  private void applyWritesFromPartialBlockAccessView(
      final PartialBlockAccessView partialBlockAccessView,
      final PathBasedWorldStateUpdateAccumulator<?> worldStateUpdater) {
    for (var accountChanges : partialBlockAccessView.accountChanges()) {
      MutableAccount account = null;

      final Optional<Wei> postBalance = accountChanges.getPostBalance();
      if (postBalance.isPresent()) {
        account = worldStateUpdater.getOrCreate(accountChanges.getAddress());
        account.setBalance(postBalance.get());
      }

      final Optional<Long> nonceChange = accountChanges.getNonceChange();
      if (nonceChange.isPresent()) {
        if (account == null) {
          account = worldStateUpdater.getOrCreate(accountChanges.getAddress());
        }
        account.setNonce(nonceChange.get());
      }

      final Optional<Bytes> newCode = accountChanges.getNewCode();
      if (newCode.isPresent()) {
        if (account == null) {
          account = worldStateUpdater.getOrCreate(accountChanges.getAddress());
        }
        account.setCode(newCode.get());
      }

      for (var slotChange : accountChanges.getStorageChanges()) {
        final StorageSlotKey slot = slotChange.slot();
        if (account == null) {
          account = worldStateUpdater.getOrCreate(accountChanges.getAddress());
        }
        account.setStorageValue(
            slot.getSlotKey().orElseThrow(),
            slotChange.newValue() != null ? slotChange.newValue() : UInt256.ZERO);
      }
    }
  }
}
