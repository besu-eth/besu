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
package org.hyperledger.besu.ethereum.mainnet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BlockGasAccountingStrategy}.
 *
 * <p>Amsterdam (EIP-7778 + EIP-8037) changes how gas is accounted for at the block level:
 *
 * <ul>
 *   <li>Pre-Amsterdam (FRONTIER): Block gas = gasLimit - gasRemaining (post-refund)
 *   <li>Amsterdam: Block gas = pre-refund gas, split into regular and state dimensions
 * </ul>
 */
public class BlockGasAccountingStrategyTest {

  private static final long GAS_LIMIT = 100_000L;
  private static final long GAS_REMAINING = 30_000L;
  // Pre-refund gas used: gasLimit - gasRemaining = 70,000
  private static final long PRE_REFUND_GAS = GAS_LIMIT - GAS_REMAINING;

  @Test
  public void frontierStrategy_usesPostRefundGas() {
    // Setup: Transaction with gas limit 100k, 30k remaining after execution
    final Transaction tx = mock(Transaction.class);
    when(tx.getGasLimit()).thenReturn(GAS_LIMIT);

    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(GAS_REMAINING);
    when(result.getEstimateGasUsedByTransaction()).thenReturn(PRE_REFUND_GAS);

    // Frontier strategy: gasLimit - gasRemaining = 100,000 - 30,000 = 70,000
    final long blockGas =
        BlockGasAccountingStrategy.FRONTIER.calculateTransactionRegularGas(tx, result);

    assertThat(blockGas).isEqualTo(PRE_REFUND_GAS);
  }

  @Test
  public void amsterdamStrategy_usesPreRefundGas() {
    // Setup: Transaction with gas limit 100k, processed with pre-refund gas of 70k, no state gas
    final Transaction tx = mock(Transaction.class);
    when(tx.getGasLimit()).thenReturn(GAS_LIMIT);

    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(GAS_REMAINING);
    when(result.getEstimateGasUsedByTransaction()).thenReturn(PRE_REFUND_GAS);
    when(result.getStateGasUsed()).thenReturn(0L);

    // Amsterdam strategy: estimateGasUsedByTransaction - stateGasUsed = 70,000 - 0 = 70,000
    final long blockGas =
        BlockGasAccountingStrategy.AMSTERDAM.calculateTransactionRegularGas(tx, result);

    assertThat(blockGas).isEqualTo(PRE_REFUND_GAS);
  }

  @Test
  public void strategiesDifferWhenRefundsApply() {
    // Setup: Simulate a transaction with SSTORE refunds
    // - Gas limit: 100,000
    // - Gas remaining after refund applied: 40,000 (post-refund remaining)
    // - Actual execution used 70,000 gas before refunds
    // - Refund of 10,000 was applied
    final long gasRemainingAfterRefund = 40_000L;
    final long preRefundGasUsed = 70_000L;

    final Transaction tx = mock(Transaction.class);
    when(tx.getGasLimit()).thenReturn(GAS_LIMIT);

    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(gasRemainingAfterRefund);
    when(result.getEstimateGasUsedByTransaction()).thenReturn(preRefundGasUsed);
    when(result.getStateGasUsed()).thenReturn(0L);

    // Frontier: 100,000 - 40,000 = 60,000 (benefits from refund)
    final long frontierGas =
        BlockGasAccountingStrategy.FRONTIER.calculateTransactionRegularGas(tx, result);
    // Amsterdam: 70,000 (no refund benefit for block accounting)
    final long amsterdamGas =
        BlockGasAccountingStrategy.AMSTERDAM.calculateTransactionRegularGas(tx, result);

    assertThat(frontierGas).isEqualTo(60_000L);
    assertThat(amsterdamGas).isEqualTo(70_000L);
    // Amsterdam accounts for more gas, preventing block gas limit circumvention
    assertThat(amsterdamGas).isGreaterThan(frontierGas);
  }

  @Test
  public void strategiesEqualWhenNoRefunds() {
    // When there are no refunds, both strategies should produce the same result
    final long gasUsed = 50_000L;
    final long gasRemaining = GAS_LIMIT - gasUsed;

    final Transaction tx = mock(Transaction.class);
    when(tx.getGasLimit()).thenReturn(GAS_LIMIT);

    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(gasRemaining);
    when(result.getEstimateGasUsedByTransaction()).thenReturn(gasUsed);
    when(result.getStateGasUsed()).thenReturn(0L);

    final long frontierGas =
        BlockGasAccountingStrategy.FRONTIER.calculateTransactionRegularGas(tx, result);
    final long amsterdamGas =
        BlockGasAccountingStrategy.AMSTERDAM.calculateTransactionRegularGas(tx, result);

    assertThat(frontierGas).isEqualTo(gasUsed);
    assertThat(amsterdamGas).isEqualTo(gasUsed);
  }

  @Test
  public void amsterdamStrategy_subtractsStateGasFromBlockGas() {
    final Transaction tx = mock(Transaction.class);
    when(tx.getGasLimit()).thenReturn(GAS_LIMIT);

    final TransactionProcessingResult result = mock(TransactionProcessingResult.class);
    when(result.getGasRemaining()).thenReturn(GAS_REMAINING);
    // estimateGasUsedByTransaction = 70,000 (pre-refund), stateGas = 10,000
    when(result.getEstimateGasUsedByTransaction()).thenReturn(PRE_REFUND_GAS);
    when(result.getStateGasUsed()).thenReturn(10_000L);

    // Amsterdam block gas = estimateGas - stateGas = 70,000 - 10,000 = 60,000
    final long blockGas =
        BlockGasAccountingStrategy.AMSTERDAM.calculateTransactionRegularGas(tx, result);
    assertThat(blockGas).isEqualTo(60_000L);
  }

  @Test
  public void amsterdamStrategy_effectiveGasUsedIsMaxOfDimensions() {
    // max(regular=50k, state=80k) = 80k
    assertThat(BlockGasAccountingStrategy.AMSTERDAM.effectiveGasUsed(50_000L, 80_000L))
        .isEqualTo(80_000L);
    // max(regular=80k, state=50k) = 80k
    assertThat(BlockGasAccountingStrategy.AMSTERDAM.effectiveGasUsed(80_000L, 50_000L))
        .isEqualTo(80_000L);
    // Frontier always returns regular gas only
    assertThat(BlockGasAccountingStrategy.FRONTIER.effectiveGasUsed(50_000L, 80_000L))
        .isEqualTo(50_000L);
  }

  @Test
  public void amsterdamStrategy_hasBlockCapacityChecksRegularGasOnly() {
    final long blockGasLimit = 100_000L;
    // Regular used: 60k, remaining regular = 40k
    // State gas is NOT checked in hasBlockCapacity (only validated at block level)
    assertThat(
            BlockGasAccountingStrategy.AMSTERDAM.hasBlockCapacity(
                40_000L, 60_000L, 40_000L, blockGasLimit))
        .isTrue();
    // txGasLimit=40001 > remaining_regular=40k, exceeds regular capacity
    assertThat(
            BlockGasAccountingStrategy.AMSTERDAM.hasBlockCapacity(
                40_001L, 60_000L, 40_000L, blockGasLimit))
        .isFalse();

    // Even when state gas is high, only regular headroom matters
    // Regular used: 40k, State used: 80k, remaining regular = 60k
    assertThat(
            BlockGasAccountingStrategy.AMSTERDAM.hasBlockCapacity(
                30_000L, 40_000L, 80_000L, blockGasLimit))
        .isTrue();

    // When state gas is 0, only regular headroom matters: 100k - 60k = 40k
    assertThat(
            BlockGasAccountingStrategy.AMSTERDAM.hasBlockCapacity(
                40_000L, 60_000L, 0L, blockGasLimit))
        .isTrue();
    assertThat(
            BlockGasAccountingStrategy.AMSTERDAM.hasBlockCapacity(
                40_001L, 60_000L, 0L, blockGasLimit))
        .isFalse();

    assertThat(
            BlockGasAccountingStrategy.FRONTIER.hasBlockCapacity(
                40_000L, 60_000L, 0L, blockGasLimit))
        .isTrue();
    assertThat(
            BlockGasAccountingStrategy.FRONTIER.hasBlockCapacity(
                40_001L, 60_000L, 0L, blockGasLimit))
        .isFalse();
  }
}
