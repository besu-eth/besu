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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ParallelBlockTransactionProcessorTest {

  @Test
  void recoversTheSenderOnTheBlockTransactionBeforeCopyingIt() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSender(transaction))
        .isSameAs(copy);

    final InOrder order = inOrder(transaction);
    order.verify(transaction).getSender();
    order.verify(transaction).detachedCopy();
  }

  @Test
  void copiesATransactionWhoseSignatureRecoversNoSender() {
    final Transaction transaction = mock(Transaction.class);
    final Transaction copy = mock(Transaction.class);
    when(transaction.getSender()).thenThrow(new IllegalStateException("no public key"));
    when(transaction.detachedCopy()).thenReturn(copy);

    assertThat(ParallelBlockTransactionProcessor.detachedCopyWithSender(transaction))
        .isSameAs(copy);
  }

  @Test
  void runsEveryTransactionOnceOnTheExecutorsThreads() throws Exception {
    final List<Integer> ran = Collections.synchronizedList(new ArrayList<>());
    final RecordingProcessor processor = new RecordingProcessor(ran);
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      processor.runAsyncBlock(
          null,
          null,
          transactions(1_000),
          null,
          null,
          null,
          executor,
          Optional.empty(),
          Optional.empty());

      for (int i = 0; i < 1_000; i++) {
        assertThat(processor.removeFuture(i).get(10, TimeUnit.SECONDS)).isNull();
      }
    } finally {
      executor.shutdownNow();
    }
    assertThat(ran).hasSize(1_000).containsExactlyInAnyOrderElementsOf(range(1_000));
  }

  @Test
  void skipsTheTransactionsOfAnAbortedBlock() {
    final List<Integer> ran = Collections.synchronizedList(new ArrayList<>());
    final RecordingProcessor processor = new RecordingProcessor(ran);
    final List<Runnable> queued = new ArrayList<>();

    processor.runAsyncBlock(
        null,
        null,
        transactions(10),
        null,
        null,
        null,
        queued::add,
        Optional.empty(),
        Optional.empty());
    processor.abort();
    queued.forEach(Runnable::run);

    assertThat(ran).isEmpty();
  }

  @Test
  void failsTheFutureOfATransactionThatThrowsAndRunsTheOthers() {
    final ParallelBlockTransactionProcessor processor =
        new RecordingProcessor(new ArrayList<>()) {
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
            if (transactionLocation == 1) {
              throw new IllegalStateException("simulated failure");
            }
            return null;
          }
        };

    processor.runAsyncBlock(
        null,
        null,
        transactions(3),
        null,
        null,
        null,
        Runnable::run,
        Optional.empty(),
        Optional.empty());

    assertThat(processor.removeFuture(0)).isCompleted();
    assertThat(processor.removeFuture(1)).isCompletedExceptionally();
    assertThat(processor.removeFuture(2)).isCompleted();
  }

  private static List<Transaction> transactions(final int count) {
    return range(count).stream().map(i -> mock(Transaction.class)).toList();
  }

  private static List<Integer> range(final int count) {
    return IntStream.range(0, count).boxed().toList();
  }

  /** Records the index of each transaction it runs, with no result. */
  private static class RecordingProcessor extends ParallelBlockTransactionProcessor {
    private final List<Integer> ran;

    RecordingProcessor(final List<Integer> ran) {
      this.ran = ran;
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
      ran.add(transactionLocation);
      return null;
    }

    @Override
    public Optional<TransactionProcessingResult> getProcessingResult(
        final MutableWorldState worldState,
        final Address miningBeneficiary,
        final Transaction transaction,
        final int location,
        final Optional<Counter> confirmedParallelizedTransactionCounter,
        final Optional<Counter> conflictingButCachedTransactionCounter) {
      return Optional.empty();
    }
  }
}
