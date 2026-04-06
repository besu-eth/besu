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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.MutableWorldState;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

/**
 * Integration tests for BAL (Block Access List) based parallel block processing. Uses
 * BalConcurrentTransactionProcessor under the hood.
 *
 * <p>BAL-specific flow:
 *
 * <ol>
 *   <li>Sequential execution generates the BAL + reference state root
 *   <li>Parallel import uses the BAL from step 1 with BalConcurrentTransactionProcessor
 *   <li>State roots are compared
 * </ol>
 */
class BalParallelBlockProcessorIntegrationTest {

  private static String getVariant() {
    return "BAL";
  }

  private static ParallelTransactionPreprocessing createPreprocessing(
      final MainnetTransactionProcessor transactionProcessor) {
    return new ParallelTransactionPreprocessing(
        transactionProcessor, Runnable::run, BalConfiguration.DEFAULT);
  }

  /** Base class for BAL tests that overrides executeAndCompare with BAL-specific logic. */
  abstract static class BalTestBase extends AbstractParallelBlockProcessorIntegrationTest {

    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }

    /**
     * BAL-specific comparison:
     *
     * <ol>
     *   <li>Discover the correct state root
     *   <li>Sequential execution generates the BAL
     *   <li>Parallel import uses the generated BAL with BalConcurrentTransactionProcessor
     *   <li>Compare state roots
     * </ol>
     */
    @Override
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      final Hash stateRoot = discoverStateRoot(baseFee, txs);

      // ========== Step 1: Sequential execution ==========
      final ExecutionContextTestFixture seqCtx = createFreshContext();
      final MutableWorldState seqWs = seqCtx.getStateArchive().getWorldState();
      final Block block = createBlock(seqCtx, stateRoot, baseFee, txs);
      final ProtocolSpec spec =
          seqCtx
              .getProtocolSchedule()
              .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
      final MainnetTransactionProcessor txProcessor = spec.getTransactionProcessor();

      final BlockProcessor seqProcessor =
          new MainnetBlockProcessor(
              txProcessor,
              spec.getTransactionReceiptFactory(),
              Wei.ZERO,
              BlockHeader::getCoinbase,
              true,
              seqCtx.getProtocolSchedule(),
              SEQUENTIAL_CONFIG);

      final BlockProcessingResult seqResult =
          seqProcessor.processBlock(
              seqCtx.getProtocolContext(), seqCtx.getBlockchain(), seqWs, block);
      assertTrue(
          seqResult.isSuccessful(),
          "Sequential execution failed: " + seqResult.errorMessage.orElse("(no message)"));

      final Optional<BlockAccessList> generatedBal = getBlockAccessList(seqResult);
      assertThat(generatedBal).as("Sequential execution should produce a BAL").isPresent();

      // ========== Step 2: Parallel import using BAL ==========
      final ExecutionContextTestFixture parCtx = createFreshContext();
      final MutableWorldState parWs = parCtx.getStateArchive().getWorldState();
      final Block parBlock = createBlock(parCtx, stateRoot, baseFee, txs);
      final ProtocolSpec parSpec =
          parCtx
              .getProtocolSchedule()
              .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
      final MainnetTransactionProcessor parTxProcessor = parSpec.getTransactionProcessor();

      final BlockProcessor parProcessor = createParallelProcessor(parCtx);
      final ParallelTransactionPreprocessing balImportPreprocessing =
          new ParallelTransactionPreprocessingWithBal(
              parTxProcessor, generatedBal.get(), BalConfiguration.DEFAULT);

      final BlockProcessingResult parResult =
          parProcessor.processBlock(
              parCtx.getProtocolContext(),
              parCtx.getBlockchain(),
              parWs,
              parBlock,
              balImportPreprocessing);
      assertTrue(
          parResult.isSuccessful(),
          "BAL parallel import failed: " + parResult.errorMessage.orElse("(no message)"));

      // ========== Step 3: Compare state roots ==========
      assertThat(parWs.rootHash())
          .as("BAL parallel import state root must match sequential reference")
          .isEqualTo(seqWs.rootHash());

      final Optional<BlockAccessList> parBal = getBlockAccessList(parResult);
      assertThat(parBal).as("Parallel import should also produce a BAL").isPresent();

      final Hash seqBalHash = BodyValidation.balHash(generatedBal.get());
      final Hash parBalHash = BodyValidation.balHash(parBal.get());
      assertThat(parBalHash)
          .as("BAL hash from parallel import must match sequential")
          .isEqualTo(seqBalHash);

      return new ComparisonResult(
          seqWs.rootHash(), parWs.rootHash(), seqResult, parResult, seqWs, parWs);
    }
  }

  /**
   * Custom preprocessing that injects a pre-computed BAL to force use of
   * BalConcurrentTransactionProcessor with applyWritesFromPriorTransactions.
   */
  private static class ParallelTransactionPreprocessingWithBal
      extends ParallelTransactionPreprocessing {

    private final BlockAccessList preComputedBal;

    ParallelTransactionPreprocessingWithBal(
        final MainnetTransactionProcessor transactionProcessor,
        final BlockAccessList preComputedBal,
        final BalConfiguration balConfiguration) {
      super(transactionProcessor, Runnable::run, balConfiguration);
      this.preComputedBal = preComputedBal;
    }

    @Override
    public Optional<PreprocessingContext> run(
        final ProtocolContext protocolContext,
        final BlockHeader blockHeader,
        final List<Transaction> transactions,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final Optional<BlockAccessList.BlockAccessListBuilder> blockAccessListBuilder,
        final Optional<BlockAccessList> maybeBlockBal) {
      return super.run(
          protocolContext,
          blockHeader,
          transactions,
          miningBeneficiary,
          blockHashLookup,
          blobGasPrice,
          blockAccessListBuilder,
          Optional.of(preComputedBal));
    }
  }

  @Nested
  @DisplayName("Simple Transfers")
  class SimpleTransfers extends AbstractSimpleTransferTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }

    @Override
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }
  }

  @Nested
  @DisplayName("Contract Storage")
  class ContractStorage extends AbstractContractStorageTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }

    @Override
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }
  }

  @Nested
  @DisplayName("Storage Dependency (Collision Detection)")
  class StorageDependency extends AbstractStorageDependencyTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }

    @Override
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }
  }

  @Nested
  @DisplayName("Mining Beneficiary BAL")
  class MiningBeneficiaryBal extends AbstractMiningBeneficiaryBalTest {
    @Override
    protected String getVariantName() {
      return getVariant();
    }

    @Override
    protected ParallelTransactionPreprocessing createParallelPreprocessing(
        final MainnetTransactionProcessor transactionProcessor) {
      return createPreprocessing(transactionProcessor);
    }

    @Override
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }
  }
}
