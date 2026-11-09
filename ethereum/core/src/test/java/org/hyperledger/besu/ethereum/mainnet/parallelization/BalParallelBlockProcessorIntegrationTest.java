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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.config.JsonUtil;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.ExecutionContextTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockExecutionContext;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.CodeChange;
import org.hyperledger.besu.ethereum.worldstate.WorldStateQueryParams;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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

  /** Base class for BAL tests that overrides executeAndCompare with BAL-specific logic. */
  abstract static class BalTestBase extends AbstractParallelBlockProcessorIntegrationTest {

    @Override
    protected String getVariantName() {
      return getVariant();
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
              BlockHeader::getCoinbase,
              seqCtx.getProtocolSchedule(),
              SEQUENTIAL_CONFIG);

      final BlockProcessingResult seqResult =
          seqProcessor.processBlock(
              BlockExecutionContext.builder()
                  .protocolContext(seqCtx.getProtocolContext())
                  .worldState(seqWs)
                  .block(block)
                  .blockAccessList(Optional.empty())
                  .build());
      assertTrue(
          seqResult.isSuccessful(),
          "Sequential execution failed: " + seqResult.errorMessage.orElse("(no message)"));

      final Optional<BlockAccessList> generatedBal = getBlockAccessList(seqResult);
      assertThat(generatedBal).as("Sequential execution should produce a BAL").isPresent();

      // ========== Step 2: Parallel import using BAL ==========
      final ExecutionContextTestFixture parCtx = createFreshContext();
      final MutableWorldState parWs = parCtx.getStateArchive().getWorldState();
      final Block parBlock = createBlock(parCtx, stateRoot, baseFee, txs);
      final BlockProcessor parProcessor =
          new ParallelExecutionWithPrecomputedBal(parCtx, generatedBal.get());

      final BlockProcessingResult parResult =
          parProcessor.processBlock(
              BlockExecutionContext.builder()
                  .protocolContext(parCtx.getProtocolContext())
                  .worldState(parWs)
                  .block(parBlock)
                  .blockAccessList(Optional.empty())
                  .build());
      assertTrue(
          parResult.isSuccessful(),
          "BAL parallel import failed: " + parResult.errorMessage.orElse("(no message)"));
      assertParallelizationRecordedInResults(seqResult, parResult, txs.length);

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

    @Override
    protected ComparisonResult executeAndCompareParentOfChainHead(
        final Wei baseFee, final Transaction... txs) {
      final ExecutionContextTestFixture discoveryCtx = createFreshContext();
      advanceCanonicalChainWithOneEmptyBlock(discoveryCtx, baseFee);
      final Hash stateRoot =
          discoverStateRootAtParent(
              discoveryCtx, parentOfChainHead(discoveryCtx), baseFee, MINING_BENEFICIARY, txs);

      final ExecutionContextTestFixture seqCtx = createFreshContext();
      advanceCanonicalChainWithOneEmptyBlock(seqCtx, baseFee);
      final BlockHeader seqTxParent = parentOfChainHead(seqCtx);
      final MutableWorldState seqWs =
          seqCtx
              .getStateArchive()
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(seqTxParent))
              .orElseThrow();
      final Block seqBlock =
          createBlock(seqCtx, seqTxParent, stateRoot, baseFee, MINING_BENEFICIARY, txs);
      final ProtocolSpec spec =
          seqCtx
              .getProtocolSchedule()
              .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
      final MainnetTransactionProcessor txProcessor = spec.getTransactionProcessor();

      final BlockProcessor seqProcessor =
          new MainnetBlockProcessor(
              txProcessor,
              spec.getTransactionReceiptFactory(),
              BlockHeader::getCoinbase,
              seqCtx.getProtocolSchedule(),
              SEQUENTIAL_CONFIG);

      final BlockProcessingResult seqResult =
          seqProcessor.processBlock(
              BlockExecutionContext.builder()
                  .protocolContext(seqCtx.getProtocolContext())
                  .worldState(seqWs)
                  .block(seqBlock)
                  .blockAccessList(Optional.empty())
                  .build());
      assertTrue(
          seqResult.isSuccessful(),
          "Sequential execution failed: " + seqResult.errorMessage.orElse("(no message)"));

      final Optional<BlockAccessList> generatedBal = getBlockAccessList(seqResult);
      assertThat(generatedBal).as("Sequential execution should produce a BAL").isPresent();

      final ExecutionContextTestFixture parCtx = createFreshContext();
      advanceCanonicalChainWithOneEmptyBlock(parCtx, baseFee);
      final BlockHeader parTxParent = parentOfChainHead(parCtx);
      final MutableWorldState parWs =
          parCtx
              .getStateArchive()
              .getWorldState(WorldStateQueryParams.withBlockHeaderAndUpdateNodeHead(parTxParent))
              .orElseThrow();
      final Block parBlock =
          createBlock(parCtx, parTxParent, stateRoot, baseFee, MINING_BENEFICIARY, txs);
      final BlockProcessor parProcessor =
          new ParallelExecutionWithPrecomputedBal(parCtx, generatedBal.get());

      final BlockProcessingResult parResult =
          parProcessor.processBlock(
              BlockExecutionContext.builder()
                  .protocolContext(parCtx.getProtocolContext())
                  .worldState(parWs)
                  .block(parBlock)
                  .blockAccessList(Optional.empty())
                  .build());
      assertTrue(
          parResult.isSuccessful(),
          "BAL parallel import failed: " + parResult.errorMessage.orElse("(no message)"));
      assertParallelizationRecordedInResults(seqResult, parResult, txs.length);

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
   * Parallel execution that injects a pre-computed BAL into the parallel run to force use of
   * BalConcurrentTransactionProcessor with applyWritesFromPriorTransactions.
   */
  private static class ParallelExecutionWithPrecomputedBal
      extends NoFallbackParallelBlockProcessor {

    private final BlockAccessList preComputedBal;

    ParallelExecutionWithPrecomputedBal(
        final ExecutionContextTestFixture ctx, final BlockAccessList preComputedBal) {
      this(
          ctx.getProtocolSchedule()
              .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader()),
          ctx.getProtocolSchedule(),
          preComputedBal);
    }

    private ParallelExecutionWithPrecomputedBal(
        final ProtocolSpec spec,
        final ProtocolSchedule protocolSchedule,
        final BlockAccessList preComputedBal) {
      super(
          spec.getTransactionProcessor(),
          spec.getTransactionReceiptFactory(),
          BlockHeader::getCoinbase,
          protocolSchedule,
          BalConfiguration.DEFAULT);
      this.preComputedBal = preComputedBal;
    }

    @Override
    protected Optional<ParallelBlockTransactionProcessor> startParallelExecution(
        final ProtocolContext protocolContext,
        final BlockHeader blockHeader,
        final List<Transaction> transactions,
        final Address miningBeneficiary,
        final BlockHashLookup blockHashLookup,
        final Wei blobGasPrice,
        final Optional<BlockAccessList.BlockAccessListBuilder> blockAccessListBuilder,
        final Optional<BlockAccessList> blockAccessList,
        final Optional<BlockHeader> maybeParentHeader) {
      return super.startParallelExecution(
          protocolContext,
          blockHeader,
          transactions,
          miningBeneficiary,
          blockHashLookup,
          blobGasPrice,
          blockAccessListBuilder,
          Optional.of(preComputedBal),
          maybeParentHeader);
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
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }

    @Override
    protected ComparisonResult executeAndCompareParentOfChainHead(
        final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompareParentOfChainHead(baseFee, txs);
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
    protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
      return new BalTestBase() {}.executeAndCompare(baseFee, txs);
    }
  }

  @Nested
  @DisplayName("EIP-8298 SETCODEFROM")
  class SetCodeFrom extends BalTestBase {

    private static final Address SOURCE =
        Address.fromHexStringStrict("0x" + "00".repeat(18) + "8298");
    private static final Address ADOPTER =
        Address.fromHexStringStrict("0x" + "00".repeat(18) + "a7a7");
    // PUSH1 1 PUSH1 0 SSTORE STOP
    private static final Bytes SOURCE_CODE = Bytes.fromHexString("0x600160005500");
    // PUSH20 SOURCE SETCODEFROM POP STOP
    private static final Bytes ADOPTER_CODE =
        Bytes.concatenate(Bytes.of(0x73), SOURCE.getBytes(), Bytes.fromHexString("0xa75000"));

    @Override
    protected ExecutionContextTestFixture createFreshContext() {
      final ObjectNode genesis =
          JsonUtil.objectNodeFromURL(
              BalParallelBlockProcessorIntegrationTest.class.getResource(GENESIS_CONFIG), false);
      ((ObjectNode) genesis.get("config")).put("bogotaTime", 0);
      final ObjectNode alloc = (ObjectNode) genesis.get("alloc");
      alloc
          .putObject(SOURCE.toHexString())
          .put("balance", "0x0")
          .put("code", SOURCE_CODE.toHexString());
      alloc
          .putObject(ADOPTER.toHexString())
          .put("balance", "0x0")
          .put("code", ADOPTER_CODE.toHexString());
      return ExecutionContextTestFixture.builder(GenesisConfig.fromConfig(genesis))
          .dataStorageFormat(DataStorageFormat.BONSAI)
          .build();
    }

    @Test
    @DisplayName("A later transaction runs the adopted code, resolved from the hash-form entry")
    void parallelImportResolvesAdoptedCodeFromThePreBlockState() {
      final Transaction adopt =
          createTransferTransaction(
              0, 0, 3_000_000, 0, 5, ADOPTER.toHexString(), ACCOUNT_GENESIS_1_KEYPAIR);
      final Transaction runAdopted =
          createTransferTransaction(
              0, 0, 3_000_000, 0, 5, ADOPTER.toHexString(), ACCOUNT_GENESIS_2_KEYPAIR);

      final ComparisonResult result = executeAndCompare(Wei.of(5), adopt, runAdopted);

      final BlockAccessList bal = getBlockAccessList(result.seqResult()).orElseThrow();
      assertThat(
              bal.accountChanges().stream()
                  .filter(changes -> changes.address().equals(ADOPTER))
                  .flatMap(changes -> changes.codeChanges().stream())
                  .toList())
          .containsExactly(CodeChange.adoptedCode(1, Hash.hash(SOURCE_CODE)));
      for (final MutableWorldState worldState :
          List.of(result.seqWorldState(), result.parWorldState())) {
        assertThat(worldState.get(ADOPTER).getCode()).isEqualTo(SOURCE_CODE);
        assertContractStorage(worldState, ADOPTER, 0, 1);
      }
    }
  }
}
