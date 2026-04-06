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
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.GENESIS_CONFIG;
import static org.hyperledger.besu.ethereum.mainnet.parallelization.ParallelBlockProcessorTestSupport.MINING_BENEFICIARY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.BlockProcessingOutputs;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
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
import org.hyperledger.besu.ethereum.mainnet.MiningBeneficiaryCalculator;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BalanceChange;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;

/**
 * Abstract base class for parallel block processor integration tests. Provides common utilities for
 * block construction, state root discovery, and sequential vs parallel comparison. Subclasses
 * provide specific parallel preprocessing implementations (BAL or Optimistic).
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractParallelBlockProcessorIntegrationTest {

  protected static final BalConfiguration SEQUENTIAL_CONFIG = BalConfiguration.DEFAULT;

  protected abstract String getVariantName();

  protected abstract ParallelTransactionPreprocessing createParallelPreprocessing(
      MainnetTransactionProcessor transactionProcessor);

  protected BalConfiguration getBalConfiguration() {
    return BalConfiguration.DEFAULT;
  }

  // ==================== Context Creation ====================

  protected ExecutionContextTestFixture createFreshContext() {
    return ExecutionContextTestFixture.builder(GenesisConfig.fromResource(GENESIS_CONFIG))
        .dataStorageFormat(DataStorageFormat.BONSAI)
        .build();
  }

  protected BlockProcessor createSequentialProcessor(final ExecutionContextTestFixture ctx) {
    final ProtocolSpec spec =
        ctx.getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    return new MainnetBlockProcessor(
        spec.getTransactionProcessor(),
        spec.getTransactionReceiptFactory(),
        Wei.ZERO,
        BlockHeader::getCoinbase,
        true,
        ctx.getProtocolSchedule(),
        SEQUENTIAL_CONFIG);
  }

  protected BlockProcessor createParallelProcessor(final ExecutionContextTestFixture ctx) {
    final ProtocolSpec spec =
        ctx.getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    return new NoBlockFallbackParallelBlockProcessor(
        spec.getTransactionProcessor(),
        spec.getTransactionReceiptFactory(),
        Wei.ZERO,
        BlockHeader::getCoinbase,
        true,
        ctx.getProtocolSchedule(),
        getBalConfiguration(),
        new NoOpMetricsSystem());
  }

  /**
   * Parallel block processor without block-level fallback. When parallel processing produces a
   * wrong state root, the result is returned as-is rather than silently falling back to sequential.
   */
  static class NoBlockFallbackParallelBlockProcessor extends MainnetParallelBlockProcessor {

    public NoBlockFallbackParallelBlockProcessor(
        final MainnetTransactionProcessor transactionProcessor,
        final TransactionReceiptFactory transactionReceiptFactory,
        final Wei blockReward,
        final MiningBeneficiaryCalculator miningBeneficiaryCalculator,
        final boolean skipZeroBlockRewards,
        final ProtocolSchedule protocolSchedule,
        final BalConfiguration balConfiguration,
        final MetricsSystem metricsSystem) {
      super(
          transactionProcessor,
          transactionReceiptFactory,
          blockReward,
          miningBeneficiaryCalculator,
          skipZeroBlockRewards,
          protocolSchedule,
          balConfiguration,
          metricsSystem);
    }

    @Override
    public BlockProcessingResult processBlock(
        final ProtocolContext protocolContext,
        final Blockchain blockchain,
        final MutableWorldState worldState,
        final Block block,
        final Optional<BlockAccessList> blockAccessList) {
      return super.processBlock(
          protocolContext,
          blockchain,
          worldState,
          block,
          blockAccessList,
          new ParallelTransactionPreprocessing(
              transactionProcessor, Runnable::run, balConfiguration));
    }
  }

  // ==================== Block Construction ====================

  protected Block createBlock(
      final ExecutionContextTestFixture ctx,
      final Hash stateRoot,
      final Wei baseFee,
      final Transaction... txs) {
    return createBlock(ctx, stateRoot, baseFee, MINING_BENEFICIARY, txs);
  }

  protected Block createBlock(
      final ExecutionContextTestFixture ctx,
      final Hash stateRoot,
      final Wei baseFee,
      final Address coinbase,
      final Transaction... txs) {
    final BlockHeader parentHeader = ctx.getBlockchain().getChainHeadHeader();
    final BlockHeader blockHeader =
        new BlockHeaderTestFixture()
            .number(parentHeader.getNumber() + 1L)
            .parentHash(parentHeader.getHash())
            .coinbase(coinbase)
            .stateRoot(stateRoot)
            .gasLimit(30_000_000L)
            .baseFeePerGas(baseFee)
            .buildHeader();
    final BlockBody blockBody =
        new BlockBody(Arrays.asList(txs), Collections.emptyList(), Optional.empty());
    return new Block(blockHeader, blockBody);
  }

  // ==================== State Root Discovery ====================

  protected Hash discoverStateRoot(final Wei baseFee, final Transaction... txs) {
    return discoverStateRoot(baseFee, MINING_BENEFICIARY, txs);
  }

  protected Hash discoverStateRoot(
      final Wei baseFee, final Address coinbase, final Transaction... txs) {
    final ExecutionContextTestFixture ctx = createFreshContext();
    final MutableWorldState ws = ctx.getStateArchive().getWorldState();
    final Block block = createBlock(ctx, Hash.ZERO, baseFee, coinbase, txs);
    final BlockProcessor processor = createSequentialProcessor(ctx);
    final BlockProcessingResult result =
        processor.processBlock(ctx.getProtocolContext(), ctx.getBlockchain(), ws, block);

    if (result.isSuccessful()) {
      return ws.rootHash();
    }

    final String msg =
        result.errorMessage.orElseThrow(
            () -> new AssertionError("Discovery processing failed without error message"));
    final String marker = "calculated ";
    final int idx = msg.indexOf(marker);
    if (idx < 0) {
      throw new AssertionError("Unexpected error message format: " + msg);
    }
    return Hash.fromHexString(msg.substring(idx + marker.length()));
  }

  // ==================== Core Comparison ====================

  protected ComparisonResult executeAndCompare(final Wei baseFee, final Transaction... txs) {
    final Hash stateRoot = discoverStateRoot(baseFee, txs);

    // Sequential processing
    final ExecutionContextTestFixture seqCtx = createFreshContext();
    final MutableWorldState seqWs = seqCtx.getStateArchive().getWorldState();
    final Block block = createBlock(seqCtx, stateRoot, baseFee, txs);
    final BlockProcessor seqProcessor = createSequentialProcessor(seqCtx);
    final BlockProcessingResult seqResult =
        seqProcessor.processBlock(
            seqCtx.getProtocolContext(), seqCtx.getBlockchain(), seqWs, block);
    assertTrue(
        seqResult.isSuccessful(),
        "Sequential processing failed: " + seqResult.errorMessage.orElse("(no message)"));

    // Parallel processing
    final ExecutionContextTestFixture parCtx = createFreshContext();
    final MutableWorldState parWs = parCtx.getStateArchive().getWorldState();
    final Block parBlock = createBlock(parCtx, stateRoot, baseFee, txs);
    final ProtocolSpec spec =
        parCtx
            .getProtocolSchedule()
            .getByBlockHeader(new BlockHeaderTestFixture().number(0L).buildHeader());
    final BlockProcessor parProcessor = createParallelProcessor(parCtx);
    final ParallelTransactionPreprocessing preprocessing =
        createParallelPreprocessing(spec.getTransactionProcessor());
    final BlockProcessingResult parResult =
        parProcessor.processBlock(
            parCtx.getProtocolContext(), parCtx.getBlockchain(), parWs, parBlock, preprocessing);
    assertTrue(
        parResult.isSuccessful(),
        getVariantName()
            + " parallel processing failed: "
            + parResult.errorMessage.orElse("(no message)"));

    // State root comparison
    assertThat(parWs.rootHash())
        .as(getVariantName() + " parallel state root must match sequential")
        .isEqualTo(seqWs.rootHash());

    // BAL hash comparison
    final Optional<BlockAccessList> seqBal = getBlockAccessList(seqResult);
    final Optional<BlockAccessList> parBal = getBlockAccessList(parResult);
    assertThat(seqBal).as("Sequential BAL should be present").isPresent();
    assertThat(parBal).as(getVariantName() + " parallel BAL should be present").isPresent();
    assertThat(BodyValidation.balHash(parBal.get()))
        .as(getVariantName() + " parallel BAL hash must match sequential")
        .isEqualTo(BodyValidation.balHash(seqBal.get()));

    return new ComparisonResult(
        seqWs.rootHash(), parWs.rootHash(), seqResult, parResult, seqWs, parWs);
  }

  // ==================== Transaction Builders ====================

  protected Transaction createTransferTransaction(
      final long nonce,
      final long value,
      final long gasLimit,
      final long maxPriorityFeePerGas,
      final long maxFeePerGas,
      final String toAddress,
      final KeyPair keyPair) {
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(maxPriorityFeePerGas))
        .maxFeePerGas(Wei.of(maxFeePerGas))
        .gasLimit(gasLimit)
        .to(Address.fromHexStringStrict(toAddress))
        .value(Wei.of(value))
        .payload(Bytes.EMPTY)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  protected Transaction createContractCallTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodName,
      final KeyPair keyPair,
      final Optional<Integer> value) {
    final Bytes payload = encodeFunctionCall(methodName, value);
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(0))
        .maxFeePerGas(Wei.of(5))
        .gasLimit(3_000_000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  @SuppressWarnings("SameParameterValue")
  protected Transaction createContractSendEthTransaction(
      final int nonce,
      final Address contractAddress,
      final String methodName,
      final KeyPair keyPair,
      final String toAddress,
      final long value) {
    final List<Type> inputParameters =
        Arrays.asList(new org.web3j.abi.datatypes.Address(toAddress), new Uint256(value));
    final Function function = new Function(methodName, inputParameters, List.of());
    final Bytes payload = Bytes.fromHexString(FunctionEncoder.encode(function));
    return Transaction.builder()
        .type(TransactionType.EIP1559)
        .nonce(nonce)
        .maxPriorityFeePerGas(Wei.of(0))
        .maxFeePerGas(Wei.of(5))
        .gasLimit(3_000_000L)
        .to(contractAddress)
        .value(Wei.ZERO)
        .payload(payload)
        .chainId(BigInteger.valueOf(42))
        .signAndBuild(keyPair);
  }

  @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "OptionalIsPresent"})
  private Bytes encodeFunctionCall(final String methodName, final Optional<Integer> value) {
    final List<Type> inputParameters =
        value.isPresent() ? Arrays.<Type>asList(new Uint256(value.get())) : List.of();
    final Function function = new Function(methodName, inputParameters, List.of());
    return Bytes.fromHexString(FunctionEncoder.encode(function));
  }

  // ==================== Assertions ====================

  protected void assertAccountsMatch(
      final MutableWorldState seqWs, final MutableWorldState parWs, final Address address) {
    final BonsaiAccount seqAccount = (BonsaiAccount) seqWs.get(address);
    final BonsaiAccount parAccount = (BonsaiAccount) parWs.get(address);

    if (seqAccount == null) {
      assertThat(parAccount).as("Account " + address + " should be null in both").isNull();
      return;
    }
    assertThat(parAccount).as("Account " + address + " should exist in parallel").isNotNull();
    assertThat(parAccount.getBalance())
        .as("Balance mismatch for " + address)
        .isEqualTo(seqAccount.getBalance());
    assertThat(parAccount.getNonce())
        .as("Nonce mismatch for " + address)
        .isEqualTo(seqAccount.getNonce());
  }

  protected void assertContractStorageMatches(
      final MutableWorldState seqWs,
      final MutableWorldState parWs,
      final Address contractAddress,
      final int slot) {
    final BonsaiAccount seqAccount = (BonsaiAccount) seqWs.get(contractAddress);
    final BonsaiAccount parAccount = (BonsaiAccount) parWs.get(contractAddress);
    assertThat(parAccount.getStorageValue(UInt256.valueOf(slot)))
        .as("Storage slot " + slot + " mismatch for " + contractAddress)
        .isEqualTo(seqAccount.getStorageValue(UInt256.valueOf(slot)));
  }

  protected void assertContractStorage(
      final MutableWorldState worldState,
      final Address contractAddress,
      final int slot,
      final int expectedValue) {
    final BonsaiAccount account = (BonsaiAccount) worldState.get(contractAddress);
    assertThat(account).as("Contract account should exist: " + contractAddress).isNotNull();
    assertThat(account.getStorageValue(UInt256.valueOf(slot)))
        .as("Storage slot " + slot + " value")
        .isEqualTo(UInt256.valueOf(expectedValue));
  }

  protected Optional<BlockAccessList> getBlockAccessList(final BlockProcessingResult result) {
    return result.getYield().flatMap(BlockProcessingOutputs::getBlockAccessList);
  }

  protected List<BalanceChange> getBalanceChangesFor(
      final BlockAccessList bal, final Address address) {
    return bal.accountChanges().stream()
        .filter(ac -> ac.address().equals(address))
        .flatMap(ac -> ac.balanceChanges().stream())
        .toList();
  }

  // ==================== Result Record ====================

  protected record ComparisonResult(
      Hash seqStateRoot,
      Hash parStateRoot,
      BlockProcessingResult seqResult,
      BlockProcessingResult parResult,
      MutableWorldState seqWorldState,
      MutableWorldState parWorldState) {}
}
