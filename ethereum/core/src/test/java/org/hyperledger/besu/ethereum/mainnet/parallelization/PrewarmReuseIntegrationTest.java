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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_3;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_4;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ConsensusContextFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.TransactionValidationParams;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * Blocks that take prewarm results instead of executing transactions end as blocks that execute
 * every transaction: same state root and, with an access list, the same access list.
 */
class PrewarmReuseIntegrationTest extends AbstractParallelBlockProcessorIntegrationTest {

  private static final Wei BASE_FEE = Wei.of(5);
  private static final BalConfiguration OPTIMISTIC =
      ImmutableBalConfiguration.builder().isPerfectParallelizationEnabled(false).build();

  @Override
  protected String getVariantName() {
    return "prewarm reuse";
  }

  @Test
  void blockWithAccessListTakesPrewarmResults() {
    final Transaction[] transactions = independentTransactionsAndASenderChain();
    final Reference reference = reference(transactions);

    final Imported imported =
        importBlock(reference, transactions, Optional.of(reference.blockAccessList()), true);

    assertMatches(reference, imported);
    assertThat(imported.prewarmed().reusedIn(imported.block().getHash())).isTrue();
  }

  @Test
  void blockWithoutAccessListTakesPrewarmResultsWhereNoParallelResultApplies() {
    // the sender's later transactions conflict with its first one in the parallel execution
    final Transaction[] transactions = independentTransactionsAndASenderChain();
    final Reference reference = reference(transactions);

    final Imported imported = importBlock(reference, transactions, Optional.empty(), true);

    assertMatches(reference, imported);
    assertThat(imported.prewarmed().reusedIn(imported.block().getHash())).isTrue();
  }

  @Test
  void blockWithAccessListExecutesATransactionWhoseReadsChanged() {
    final Transaction[] transactions = transactionReadingWhatAnEarlierOneChanged();
    final Reference reference = reference(transactions);

    // only the second transaction ran ahead of the block, without the first one's transfer
    final Imported imported =
        importBlock(
            reference,
            transactions,
            Optional.of(reference.blockAccessList()),
            false,
            transactions[1]);

    assertMatches(reference, imported);
    assertThat(imported.prewarmed().reusedIn(imported.block().getHash())).isFalse();
  }

  @Test
  void blockWithoutAccessListExecutesATransactionWhoseReadsChanged() {
    final Transaction[] transactions = transactionReadingWhatAnEarlierOneChanged();
    final Reference reference = reference(transactions);

    final Imported imported =
        importBlock(reference, transactions, Optional.empty(), false, transactions[1]);

    assertMatches(reference, imported);
    assertThat(imported.prewarmed().reusedIn(imported.block().getHash())).isFalse();
  }

  private Transaction[] independentTransactionsAndASenderChain() {
    return new Transaction[] {
      createTransferTransaction(
          0, 1_000_000_000_000_000_000L, 300_000L, 2L, 7L, ACCOUNT_2, ACCOUNT_GENESIS_1_KEYPAIR),
      createTransferTransaction(
          1, 2_000_000_000_000_000_000L, 300_000L, 2L, 7L, ACCOUNT_3, ACCOUNT_GENESIS_1_KEYPAIR),
      createTransferTransaction(
          0, 3_000_000_000_000_000_000L, 300_000L, 3L, 8L, ACCOUNT_4, ACCOUNT_GENESIS_2_KEYPAIR)
    };
  }

  /** The second transaction spends from the account the first one pays. */
  private Transaction[] transactionReadingWhatAnEarlierOneChanged() {
    return new Transaction[] {
      createTransferTransaction(
          0,
          1_000_000_000_000_000_000L,
          300_000L,
          2L,
          7L,
          ACCOUNT_GENESIS_2,
          ACCOUNT_GENESIS_1_KEYPAIR),
      createTransferTransaction(
          0, 3_000_000_000_000_000_000L, 300_000L, 3L, 8L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR)
    };
  }

  private record Reference(
      Hash stateRoot, MutableWorldState worldState, BlockAccessList blockAccessList) {}

  private record Imported(
      Block block,
      BlockProcessingResult result,
      MutableWorldState worldState,
      PrewarmedTransactions prewarmed) {}

  /** The block executed transaction by transaction. */
  private Reference reference(final Transaction... transactions) {
    final Hash stateRoot = discoverStateRoot(BASE_FEE, transactions);
    final ExecutionContextTestFixture ctx = createFreshContext();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result =
        createSequentialProcessor(ctx)
            .processBlock(
                BlockExecutionContext.builder()
                    .protocolContext(ctx.getProtocolContext())
                    .worldState(worldState)
                    .block(createBlock(ctx, stateRoot, BASE_FEE, transactions))
                    .blockAccessList(Optional.empty())
                    .build());
    assertThat(result.isSuccessful()).as(result.errorMessage.orElse("")).isTrue();
    return new Reference(stateRoot, worldState, getBlockAccessList(result).orElseThrow());
  }

  private Imported importBlock(
      final Reference reference,
      final Transaction[] transactions,
      final Optional<BlockAccessList> blockAccessList,
      final boolean prewarmAll,
      final Transaction... prewarmOnly) {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final Block block = createBlock(ctx, reference.stateRoot(), BASE_FEE, transactions);
    final PrewarmedTransactions prewarmed =
        prewarm(ctx, block.getHeader(), prewarmAll ? transactions : prewarmOnly);
    final ProtocolContext protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(ctx.getBlockchain())
            .withWorldStateArchive(ctx.getStateArchive())
            .withConsensusContext(new ConsensusContextFixture())
            .withPrewarmedTransactions(prewarmed)
            .build();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final BlockProcessingResult result =
        processor(ctx, blockAccessList)
            .processBlock(
                BlockExecutionContext.builder()
                    .protocolContext(protocolContext)
                    .worldState(worldState)
                    .block(block)
                    .blockAccessList(Optional.empty())
                    .build());
    return new Imported(block, result, worldState, prewarmed);
  }

  private BlockProcessor processor(
      final ExecutionContextTestFixture ctx, final Optional<BlockAccessList> blockAccessList) {
    final ProtocolSpec spec =
        ctx.getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    return new NoFallbackParallelBlockProcessor(
        spec.getTransactionProcessor(),
        spec.getTransactionReceiptFactory(),
        BlockHeader::getCoinbase,
        ctx.getProtocolSchedule(),
        blockAccessList.isPresent() ? BalConfiguration.DEFAULT : OPTIMISTIC) {
      @Override
      protected Optional<ParallelBlockTransactionProcessor> startParallelExecution(
          final ProtocolContext protocolContext,
          final BlockHeader blockHeader,
          final List<Transaction> blockTransactions,
          final Address miningBeneficiary,
          final BlockHashLookup blockHashLookup,
          final Wei blobGasPrice,
          final Optional<BlockAccessListBuilder> blockAccessListBuilder,
          final Optional<BlockAccessList> ignored,
          final Optional<BlockHeader> maybeParentHeader) {
        return super.startParallelExecution(
            protocolContext,
            blockHeader,
            blockTransactions,
            miningBeneficiary,
            blockHashLookup,
            blobGasPrice,
            blockAccessListBuilder,
            blockAccessList,
            maybeParentHeader);
      }
    };
  }

  /**
   * Executes the transactions as the mempool prewarmer does: each sender's in order on the parent's
   * state, paying the prewarm coinbase and recording what they touch.
   */
  private PrewarmedTransactions prewarm(
      final ExecutionContextTestFixture ctx,
      final BlockHeader blockHeader,
      final Transaction... transactions) {
    final PrewarmedTransactions prewarmed = new PrewarmedTransactions(new NoOpMetricsSystem());
    final BlockHeader parent = ctx.getBlockchain().getChainHeadHeader();
    final ProtocolSpec spec = ctx.getProtocolSchedule().getByBlockHeader(blockHeader);
    final BlockHashLookup blockHashLookup =
        spec.getPreExecutionProcessor().createBlockHashLookup(ctx.getBlockchain(), blockHeader);
    final Map<Address, List<Transaction>> bySender = new LinkedHashMap<>();
    for (final Transaction transaction : transactions) {
      bySender.computeIfAbsent(transaction.getSender(), __ -> new ArrayList<>()).add(transaction);
    }
    for (final List<Transaction> senderTransactions : bySender.values()) {
      final MutableWorldState worldState =
          ctx.getStateArchive()
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndNoUpdateNodeHead(parent))
              .orElseThrow();
      final WorldUpdater updater = worldState.updater();
      PrewarmedTransactions.Execution previous = null;
      for (final Transaction transaction : senderTransactions) {
        final WorldUpdater transactionUpdater = updater.updater();
        final TransactionProcessingResult result =
            spec.getTransactionProcessor()
                .processTransaction(
                    transactionUpdater,
                    blockHeader,
                    transaction,
                    PrewarmedTransactions.PREWARM_COINBASE,
                    OperationTracer.NO_TRACING,
                    blockHashLookup,
                    TransactionValidationParams.processingBlock(),
                    Wei.ZERO,
                    Optional.of(BlockAccessListBuilder.createTransactionAccessLocationTracker(0)));
        assertThat(result.isSuccessful()).isTrue();
        transactionUpdater.commit();
        updater.commit();
        updater.markTransactionBoundary();
        previous =
            new PrewarmedTransactions.Execution(
                parent.getHash(),
                blockHeader.getTimestamp(),
                blockHeader.getGasLimit(),
                0,
                previous,
                result);
        prewarmed.record(transaction.getHash(), previous);
      }
    }
    return prewarmed;
  }

  private void assertMatches(final Reference reference, final Imported imported) {
    assertThat(imported.result().isSuccessful())
        .as(imported.result().errorMessage.orElse(""))
        .isTrue();
    assertThat(imported.worldState().rootHash()).isEqualTo(reference.worldState().rootHash());
    assertThat(getBlockAccessList(imported.result()).map(BodyValidation::balHash))
        .contains(BodyValidation.balHash(reference.blockAccessList()));
  }
}
