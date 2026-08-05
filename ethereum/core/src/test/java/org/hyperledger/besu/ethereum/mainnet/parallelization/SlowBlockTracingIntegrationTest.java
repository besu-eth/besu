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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_1_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.ACCOUNT_GENESIS_2_KEYPAIR;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.CONTRACT_ADDRESS;
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
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.BalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.ImmutableBalConfiguration;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockProcessor;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * End-to-end slow-block tracing over BAL block import, asserting on the JSON that actually reaches
 * the log.
 *
 * <p>The key check is that the same block traced through the sequential path and through the
 * parallel path reports the same EVM counters, writes and uniques. Those two paths collect the same
 * numbers in completely different ways — one block-level recorder versus one recorder per
 * background transaction, merged only when its result is used — so any double counting from
 * speculative execution, or any gap from discarding a used result, shows up as a mismatch.
 */
class SlowBlockTracingIntegrationTest extends AbstractParallelBlockProcessorIntegrationTest {

  private static final BalConfiguration TRACING_CONFIG =
      ImmutableBalConfiguration.builder().slowBlockThresholdMs(0L).build();

  private static final ObjectMapper JSON = new ObjectMapper();

  @Override
  protected String getVariantName() {
    return "BAL slow-block tracing";
  }

  @Override
  protected BalConfiguration getBalConfiguration() {
    return TRACING_CONFIG;
  }

  @Override
  protected ParallelTransactionPreprocessing createParallelPreprocessing(
      final MainnetTransactionProcessor transactionProcessor) {
    return new ParallelTransactionPreprocessing(
        transactionProcessor, Runnable::run, TRACING_CONFIG);
  }

  @Test
  @DisplayName("A traced BAL block emits the slow-block document with self-consistent counters")
  void emitsSelfConsistentSlowBlockDocument() {
    final JsonNode json = traceParallelImport(transactions());

    assertThat(json.get("msg").asText()).isEqualTo("Slow block");
    assertThat(json.get("block").get("tx_count").asInt()).isEqualTo(3);
    assertThat(json.get("block").get("number").asLong()).isEqualTo(1L);

    // hits are derived, never counted, so they must always reconstruct the totals
    final JsonNode reads = json.get("state_reads");
    final JsonNode cache = json.get("cache");
    assertCacheReconstructsTotal(cache.get("account"), reads.get("accounts").asLong());
    assertCacheReconstructsTotal(cache.get("storage"), reads.get("storage_slots").asLong());
    assertCacheReconstructsTotal(cache.get("code"), reads.get("code").asLong());

    // every transaction's background result was usable, so nothing was replayed
    assertThat(json.get("bal").get("replayed_txs").asInt()).isZero();
    assertThat(json.get("bal").get("tx_exec_background_ms").asDouble()).isNotNegative();

    // contract calls happened, so the EVM counters must be populated
    assertThat(json.get("evm").get("sstore").asLong()).isPositive();
    assertThat(json.get("unique").get("contracts").asLong()).isPositive();
    assertThat(json.get("unique").get("accounts").asLong()).isPositive();
    assertThat(json.get("state_writes").get("storage_slots").asLong()).isPositive();
  }

  @Test
  @DisplayName("Sequential and parallel execution of the same block report the same counters")
  void sequentialAndParallelAgreeOnBlockCounters() {
    final Transaction[] txs = transactions();
    final JsonNode sequential = traceSequentialExecution(txs);
    final JsonNode parallel = traceParallelImport(txs);

    assertThat(parallel.get("block")).isEqualTo(sequential.get("block"));
    assertThat(parallel.get("evm")).isEqualTo(sequential.get("evm"));
    assertThat(parallel.get("unique")).isEqualTo(sequential.get("unique"));
    assertThat(parallel.get("state_writes")).isEqualTo(sequential.get("state_writes"));
  }

  @Test
  @DisplayName("Nothing is logged when the threshold is negative")
  void emitsNothingWhenDisabled() {
    final List<String> logged =
        withSlowBlockLogCapture(
            () -> {
              runSequentially(BalConfiguration.DEFAULT, transactions());
              return null;
            });

    assertThat(logged).isEmpty();
  }

  private Transaction[] transactions() {
    final Address contract = Address.fromHexStringStrict(CONTRACT_ADDRESS);
    return new Transaction[] {
      createContractCallTransaction(
          0, contract, "setSlot1", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(100)),
      createContractCallTransaction(
          1, contract, "setSlot2", ACCOUNT_GENESIS_1_KEYPAIR, Optional.of(200)),
      createTransferTransaction(
          0, 1_000_000_000_000_000_000L, 300_000L, 0L, 5L, ACCOUNT_2, ACCOUNT_GENESIS_2_KEYPAIR)
    };
  }

  private void assertCacheReconstructsTotal(final JsonNode cacheNode, final long totalReads) {
    assertThat(cacheNode.get("hits").asLong()).isNotNegative();
    assertThat(cacheNode.get("hits").asLong() + cacheNode.get("misses").asLong())
        .isEqualTo(totalReads);
  }

  /** Runs the block through the sequential path with tracing on and returns the emitted JSON. */
  private JsonNode traceSequentialExecution(final Transaction... txs) {
    return onlyDocument(
        withSlowBlockLogCapture(
            () -> {
              runSequentially(TRACING_CONFIG, txs);
              return null;
            }));
  }

  /**
   * Produces the block's access list with an untraced sequential run, then imports the block in
   * parallel with tracing on and returns the emitted JSON.
   */
  private JsonNode traceParallelImport(final Transaction... txs) {
    final Wei baseFee = Wei.of(5);
    final Hash stateRoot = discoverStateRoot(baseFee, txs);
    final BlockAccessList bal = runSequentially(BalConfiguration.DEFAULT, txs);

    final ExecutionContextTestFixture ctx = createFreshContext();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final Block block = createBlock(ctx, stateRoot, baseFee, txs);
    final MainnetTransactionProcessor txProcessor = protocolSpec(ctx).getTransactionProcessor();

    final BlockProcessor processor =
        new NoBlockFallbackParallelBlockProcessor(
            txProcessor,
            protocolSpec(ctx).getTransactionReceiptFactory(),
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            ctx.getProtocolSchedule(),
            TRACING_CONFIG,
            new NoOpMetricsSystem());

    final List<String> logged =
        withSlowBlockLogCapture(
            () -> {
              final BlockProcessingResult result =
                  processor.processBlock(
                      ctx.getProtocolContext(),
                      ctx.getBlockchain(),
                      worldState,
                      block,
                      new PreprocessingWithBal(txProcessor, bal, TRACING_CONFIG));
              assertTrue(
                  result.isSuccessful(),
                  "Parallel import failed: " + result.errorMessage.orElse("(no message)"));
              return null;
            });

    return onlyDocument(logged);
  }

  /** Runs the block through the plain sequential processor and returns its access list. */
  private BlockAccessList runSequentially(
      final BalConfiguration balConfiguration, final Transaction... txs) {
    final Wei baseFee = Wei.of(5);
    final Hash stateRoot = discoverStateRoot(baseFee, txs);
    final ExecutionContextTestFixture ctx = createFreshContext();
    final MutableWorldState worldState = ctx.getStateArchive().getWorldState();
    final Block block = createBlock(ctx, stateRoot, baseFee, txs);

    final BlockProcessor processor =
        new MainnetBlockProcessor(
            protocolSpec(ctx).getTransactionProcessor(),
            protocolSpec(ctx).getTransactionReceiptFactory(),
            Wei.ZERO,
            BlockHeader::getCoinbase,
            true,
            ctx.getProtocolSchedule(),
            balConfiguration);

    final BlockProcessingResult result =
        processor.processBlock(ctx.getProtocolContext(), ctx.getBlockchain(), worldState, block);
    assertTrue(
        result.isSuccessful(),
        "Sequential execution failed: " + result.errorMessage.orElse("(no message)"));
    return getBlockAccessList(result).orElseThrow();
  }

  private ProtocolSpec protocolSpec(final ExecutionContextTestFixture ctx) {
    return ctx.getProtocolSchedule()
        .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
  }

  private JsonNode onlyDocument(final List<String> logged) {
    assertThat(logged).as("exactly one slow block document expected").hasSize(1);
    try {
      return JSON.readTree(logged.getFirst());
    } catch (final Exception e) {
      throw new AssertionError("Slow block log was not valid JSON: " + logged.getFirst(), e);
    }
  }

  /** Captures everything the dedicated slow-block logger emits while the action runs. */
  @SuppressWarnings("BannedMethod")
  private List<String> withSlowBlockLogCapture(final Supplier<Void> action) {
    final Logger logger = (Logger) LogManager.getLogger("SlowBlock");
    final List<String> messages = new CopyOnWriteArrayList<>();
    final AbstractAppender appender =
        new AbstractAppender("slow-block-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(final org.apache.logging.log4j.core.LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      action.get();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
    return messages;
  }

  /** Feeds a pre-computed access list to the parallel preprocessor, as block import would. */
  private static class PreprocessingWithBal extends ParallelTransactionPreprocessing {

    private final BlockAccessList preComputedBal;

    PreprocessingWithBal(
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
        final Optional<BlockAccessList> maybeBlockBal,
        final Optional<BlockHeader> maybeParentHeader) {
      return super.run(
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
}
