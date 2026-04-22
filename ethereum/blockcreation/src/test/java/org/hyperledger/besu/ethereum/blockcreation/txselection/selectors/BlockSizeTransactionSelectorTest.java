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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOCK_FULL;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.TX_TOO_LARGE_FOR_REMAINING_GAS;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.ethereum.mainnet.BlockGasAccountingStrategy;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import java.util.Optional;
import java.util.stream.IntStream;

import com.google.common.base.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockSizeTransactionSelectorTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEYS = SIGNATURE_ALGORITHM.generateKeyPair();
  private static final long TRANSFER_GAS_LIMIT = 21_000L;
  private static final long BLOCK_GAS_LIMIT = 1_000_000L;

  @SuppressWarnings("UnnecessaryLambda")
  private static final Supplier<Boolean> NEVER_CANCELLED = () -> false;

  @Mock(answer = RETURNS_DEEP_STUBS)
  BlockSelectionContext blockSelectionContext;

  SelectorsStateManager selectorsStateManager;
  BlockSizeTransactionSelector selector;
  MiningConfiguration miningConfiguration;

  @BeforeEach
  void setup() {
    miningConfiguration = MiningConfiguration.newDefault();
    when(blockSelectionContext.pendingBlockHeader().getGasLimit()).thenReturn(BLOCK_GAS_LIMIT);
    when(blockSelectionContext.miningConfiguration()).thenReturn(miningConfiguration);
    // Use FRONTIER strategy (gasLimit - gasRemaining) for backward compatibility with existing
    // tests
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.FRONTIER);

    selectorsStateManager = new SelectorsStateManager();
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);
  }

  @Test
  void singleTransactionBelowBlockGasLimitIsSelected() {
    final var tx = createPendingTransaction(TRANSFER_GAS_LIMIT);

    final var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext, remainingGas(0));

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(TRANSFER_GAS_LIMIT);
    assertThat(selector.getWorkingState().stateGas()).isEqualTo(0);
  }

  @Test
  void singleTransactionAboveBlockGasLimitIsNotSelected() {
    final var tx = createPendingTransaction(BLOCK_GAS_LIMIT + 1);

    final var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertNotSelected(txEvaluationContext, TX_TOO_LARGE_FOR_REMAINING_GAS);

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(0);
  }

  @Test
  void correctlyCumulatesOnlyTheEffectiveGasUsedAfterProcessing() {
    final var tx = createPendingTransaction(TRANSFER_GAS_LIMIT * 2);
    final long remainingGas = 100;

    final var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext, remainingGas(remainingGas));

    assertThat(selector.getWorkingState().regularGas())
        .isEqualTo(TRANSFER_GAS_LIMIT * 2 - remainingGas);
  }

  @Test
  void moreTransactionsBelowBlockGasLimitAreSelected() {
    selectorsStateManager.blockSelectionStarted();

    final int txCount = 10;

    IntStream.range(0, txCount)
        .forEach(
            unused -> {
              final var tx = createPendingTransaction(TRANSFER_GAS_LIMIT);

              final var txEvaluationContext =
                  new TransactionEvaluationContext(
                      blockSelectionContext.pendingBlockHeader(),
                      tx,
                      null,
                      null,
                      null,
                      NEVER_CANCELLED);
              evaluateAndAssertSelected(txEvaluationContext, remainingGas(0));
            });

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(TRANSFER_GAS_LIMIT * txCount);
  }

  @Test
  void moreTransactionsThanBlockCanFitOnlySomeAreSelected() {
    selectorsStateManager.blockSelectionStarted();

    final int txCount = 10;

    IntStream.range(0, txCount)
        .forEach(
            unused -> {
              final var tx = createPendingTransaction(TRANSFER_GAS_LIMIT);

              final var txEvaluationContext =
                  new TransactionEvaluationContext(
                      blockSelectionContext.pendingBlockHeader(),
                      tx,
                      null,
                      null,
                      null,
                      NEVER_CANCELLED);
              evaluateAndAssertSelected(txEvaluationContext, remainingGas(0));
            });

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(TRANSFER_GAS_LIMIT * txCount);

    // last tx is too big for the remaining gas
    final long tooBigGasLimit = BLOCK_GAS_LIMIT - (TRANSFER_GAS_LIMIT * txCount) + 1;

    final var bigTxEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(),
            createPendingTransaction(tooBigGasLimit),
            null,
            null,
            null,
            NEVER_CANCELLED);
    evaluateAndAssertNotSelected(bigTxEvaluationContext, TX_TOO_LARGE_FOR_REMAINING_GAS);

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(TRANSFER_GAS_LIMIT * txCount);
  }

  @Test
  void identifyWhenBlockIsFull() {
    when(blockSelectionContext.gasCalculator().getMinimumTransactionCost())
        .thenReturn(TRANSFER_GAS_LIMIT);

    selectorsStateManager.blockSelectionStarted();

    // create 2 txs, where the first fill the block leaving less gas than the min required by a
    // transfer
    final long fillBlockGasLimit = BLOCK_GAS_LIMIT - TRANSFER_GAS_LIMIT + 1;
    final var tx1 = createPendingTransaction(fillBlockGasLimit);

    final var txEvaluationContext1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    evaluateAndAssertSelected(txEvaluationContext1, remainingGas(0));

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(fillBlockGasLimit);

    final var tx2 = createPendingTransaction(TRANSFER_GAS_LIMIT);

    final var txEvaluationContext2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    evaluateAndAssertNotSelected(txEvaluationContext2, BLOCK_FULL);

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(fillBlockGasLimit);
  }

  /**
   * Tests EIP-7778 gas accounting strategy. With EIP-7778, block gas is calculated using pre-refund
   * gas (estimateGasUsedByTransaction) instead of post-refund gas (gasLimit - gasRemaining). This
   * prevents block gas limit circumvention through SSTORE refunds.
   */
  @Test
  void eip7778StrategyUsesPreRefundGasForBlockAccounting() {
    // Reconfigure with EIP-7778 strategy
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);

    // Create a transaction with gas limit 50,000
    final long txGasLimit = 50_000L;
    final var tx = createPendingTransaction(txGasLimit);

    // Simulate SSTORE refund scenario:
    // - Pre-refund gas used: 40,000 (estimateGasUsedByTransaction)
    // EIP-7778 uses only estimateGasUsedByTransaction, not gasRemaining
    final long preRefundGasUsed = 40_000L;

    final var txProcessingResult = mock(TransactionProcessingResult.class);
    when(txProcessingResult.getEstimateGasUsedByTransaction()).thenReturn(preRefundGasUsed);
    when(txProcessingResult.getStateGasUsed()).thenReturn(0L);

    final var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext, txProcessingResult);

    // EIP-7778: Should use pre-refund gas (40,000), NOT post-refund (30,000)
    assertThat(selector.getWorkingState().regularGas())
        .as("EIP-7778 should use pre-refund gas for block accounting")
        .isEqualTo(preRefundGasUsed);
  }

  /**
   * Verifies FRONTIER strategy uses post-refund gas (legacy behavior), contrasting with EIP-7778.
   */
  @Test
  void frontierStrategyUsesPostRefundGasForBlockAccounting() {
    // Use FRONTIER strategy (already set in setup, but explicit for clarity)
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.FRONTIER);
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);

    final long txGasLimit = 50_000L;
    final var tx = createPendingTransaction(txGasLimit);

    // Same scenario as above
    final long postRefundGasRemaining = 20_000L;
    final long expectedPostRefundUsed = txGasLimit - postRefundGasRemaining; // 30,000

    final var txProcessingResult = mock(TransactionProcessingResult.class);
    when(txProcessingResult.getGasRemaining()).thenReturn(postRefundGasRemaining);
    when(txProcessingResult.getStateGasUsed()).thenReturn(0L);

    final var txEvaluationContext =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx, null, null, null, NEVER_CANCELLED);
    selectorsStateManager.blockSelectionStarted();
    evaluateAndAssertSelected(txEvaluationContext, txProcessingResult);

    // FRONTIER: Should use post-refund gas (30,000)
    assertThat(selector.getWorkingState().regularGas())
        .as("FRONTIER should use post-refund gas for block accounting")
        .isEqualTo(expectedPostRefundUsed);
  }

  /**
   * EIP-8037 2D gas: Pre-processing uses per-dimension capacity check.
   *
   * <p>Scenario: block limit=30M, regular=25M used, state=5M used. The tighter dimension is regular
   * with 5M remaining, so txGasLimit must be <= 5M to pass.
   */
  @Test
  void eip8037TwoDimensionalPreProcessingChecksPerDimension() {
    when(blockSelectionContext.pendingBlockHeader().getGasLimit()).thenReturn(30_000_000L);
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);
    selectorsStateManager.blockSelectionStarted();

    // First tx: uses 25M regular, 5M state
    final var tx1 = createPendingTransaction(30_000_000L);
    final var result1 = mock(TransactionProcessingResult.class);
    when(result1.getEstimateGasUsedByTransaction()).thenReturn(30_000_000L);
    when(result1.getStateGasUsed()).thenReturn(5_000_000L);
    // calculateTransactionRegularGas returns 30M - 5M = 25M regular

    final var ctx1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx1)).isEqualTo(SELECTED);
    assertThat(selector.evaluateTransactionPostProcessing(ctx1, result1)).isEqualTo(SELECTED);

    // State: regular=25M, state=5M
    assertThat(selector.getWorkingState().regularGas()).isEqualTo(25_000_000L);
    assertThat(selector.getWorkingState().stateGas()).isEqualTo(5_000_000L);

    // tx with gasLimit=5M fits: remaining_regular = 30M - 25M = 5M >= 5M
    final var tx2 = createPendingTransaction(5_000_000L);
    final var ctx2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx2)).isEqualTo(SELECTED);

    // tx with gasLimit=5M+1 doesn't fit: exceeds tighter (regular) dimension
    final var tx3 = createPendingTransaction(5_000_001L);
    final var ctx3 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx3, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx3))
        .isEqualTo(TX_TOO_LARGE_FOR_REMAINING_GAS);
  }

  /**
   * EIP-8037 2D gas: Post-processing correctly tracks gas state after successful transactions and
   * pre-processing rejects when a dimension is exhausted.
   */
  @Test
  void eip8037PostProcessingTracksGasAndPreProcessingRejectsWhenDimensionExhausted() {
    when(blockSelectionContext.pendingBlockHeader().getGasLimit()).thenReturn(30_000_000L);
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);
    selectorsStateManager.blockSelectionStarted();

    // First tx: gasLimit=30M, uses 20M regular + 10M state = 30M total
    final var tx1 = createPendingTransaction(30_000_000L);
    final var result1 = mock(TransactionProcessingResult.class);
    when(result1.getEstimateGasUsedByTransaction()).thenReturn(30_000_000L);
    when(result1.getStateGasUsed()).thenReturn(10_000_000L);

    final var ctx1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx1)).isEqualTo(SELECTED);
    assertThat(selector.evaluateTransactionPostProcessing(ctx1, result1)).isEqualTo(SELECTED);

    // State: regular=20M, state=10M, gasMetered=max(20M,10M)=20M <= 30M ok
    assertThat(selector.getWorkingState().regularGas()).isEqualTo(20_000_000L);
    assertThat(selector.getWorkingState().stateGas()).isEqualTo(10_000_000L);

    // Second tx with gasLimit=10M fits: remaining_regular = 30M - 20M = 10M >= 10M
    final var tx2 = createPendingTransaction(10_000_000L);
    final var ctx2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx2)).isEqualTo(SELECTED);

    // Third tx with gasLimit=10M+1 doesn't fit: exceeds tighter (regular) dimension
    final var tx3 = createPendingTransaction(10_000_001L);
    final var ctx3 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx3, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx3))
        .isEqualTo(TX_TOO_LARGE_FOR_REMAINING_GAS);
  }

  /**
   * EIP-8037 2D gas: effectiveGasUsed drives occupancy/blockFull checks using max(regular,state).
   * When the tighter dimension has less remaining than min tx cost, the block is full.
   */
  @Test
  void eip8037EffectiveGasUsedDrivesBlockFullCheck() {
    when(blockSelectionContext.pendingBlockHeader().getGasLimit()).thenReturn(1_000_000L);
    when(blockSelectionContext.protocolSpec().getBlockGasAccountingStrategy())
        .thenReturn(BlockGasAccountingStrategy.AMSTERDAM);
    when(blockSelectionContext.gasCalculator().getMinimumTransactionCost())
        .thenReturn(TRANSFER_GAS_LIMIT);
    selector = new BlockSizeTransactionSelector(blockSelectionContext, selectorsStateManager);
    selectorsStateManager.blockSelectionStarted();

    // Fill block with regular dimension nearly full: gasLimit=1M, uses 990K regular + 10K state
    final var tx1 = createPendingTransaction(1_000_000L);
    final var result1 = mock(TransactionProcessingResult.class);
    when(result1.getEstimateGasUsedByTransaction()).thenReturn(1_000_000L);
    when(result1.getStateGasUsed()).thenReturn(10_000L);
    // regular gas = 1_000_000 - 10_000 = 990_000

    final var ctx1 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx1, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx1)).isEqualTo(SELECTED);
    assertThat(selector.evaluateTransactionPostProcessing(ctx1, result1)).isEqualTo(SELECTED);

    assertThat(selector.getWorkingState().regularGas()).isEqualTo(990_000L);
    assertThat(selector.getWorkingState().stateGas()).isEqualTo(10_000L);

    // tx2 gasLimit=21K > remaining_regular = 1M - 990K = 10K
    // transactionTooLargeForBlock=true; effectiveGasUsed=max(990K,10K)=990K, remaining=10K < 21K
    // result in blockFull
    final var tx2 = createPendingTransaction(TRANSFER_GAS_LIMIT);
    final var ctx2 =
        new TransactionEvaluationContext(
            blockSelectionContext.pendingBlockHeader(), tx2, null, null, null, NEVER_CANCELLED);
    assertThat(selector.evaluateTransactionPreProcessing(ctx2)).isEqualTo(BLOCK_FULL);
  }

  private void evaluateAndAssertSelected(
      final TransactionEvaluationContext txEvaluationContext,
      final TransactionProcessingResult transactionProcessingResult) {
    assertThat(selector.evaluateTransactionPreProcessing(txEvaluationContext)).isEqualTo(SELECTED);
    assertThat(
            selector.evaluateTransactionPostProcessing(
                txEvaluationContext, transactionProcessingResult))
        .isEqualTo(SELECTED);
  }

  private void evaluateAndAssertNotSelected(
      final TransactionEvaluationContext txEvaluationContext,
      final TransactionSelectionResult preProcessedResult) {
    assertThat(selector.evaluateTransactionPreProcessing(txEvaluationContext))
        .isEqualTo(preProcessedResult);
  }

  private PendingTransaction createPendingTransaction(final long gasLimit) {
    return PendingTransaction.newPendingTransaction(
        createTransaction(TransactionType.EIP1559, gasLimit), false, false, MAX_SCORE);
  }

  private Transaction createTransaction(final TransactionType type, final long gasLimit) {

    var tx =
        new TransactionTestFixture()
            .to(Optional.of(Address.fromHexString("0x634316eA0EE79c701c6F67C53A4C54cBAfd2316d")))
            .nonce(0)
            .gasLimit(gasLimit)
            .type(type)
            .maxFeePerGas(Optional.of(Wei.of(1000)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(100)));

    return tx.createTransaction(KEYS);
  }

  private TransactionProcessingResult remainingGas(final long remainingGas) {
    final var txProcessingResult = mock(TransactionProcessingResult.class);
    when(txProcessingResult.getGasRemaining()).thenReturn(remainingGas);
    when(txProcessingResult.getStateGasUsed()).thenReturn(0L);
    return txProcessingResult;
  }
}
