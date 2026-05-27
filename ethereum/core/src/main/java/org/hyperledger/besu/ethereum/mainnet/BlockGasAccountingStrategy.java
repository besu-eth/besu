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

import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;

/**
 * Strategy interface for calculating gas to add to a block's cumulative gas used. This allows
 * different hard forks to use different gas accounting methods.
 *
 * <p>Prior to Amsterdam: Block gas is calculated POST-refund (gasLimit - gasRemaining), which
 * includes the benefit of gas refunds from SSTORE operations.
 *
 * <p>Amsterdam (EIP-7778 + EIP-8037): Block gas is calculated PRE-refund and split into regular and
 * state dimensions, preventing block gas limit circumvention through refund credits.
 */
public interface BlockGasAccountingStrategy {

  /**
   * Calculate a transaction's regular gas contribution to the block's cumulative gas used.
   *
   * @param transaction the transaction being processed
   * @param result the transaction processing result
   * @return the regular gas used by the transaction for block accounting
   */
  long calculateTransactionRegularGas(Transaction transaction, TransactionProcessingResult result);

  /**
   * Check whether the block has capacity for a transaction with the given gas limit. For 1D gas
   * (pre-EIP-8037), this checks regular gas only. For 2D gas (EIP-8037), the tx gas limit must fit
   * within the remaining capacity of each dimension independently, since gas_metered =
   * max(cumulative_regular, cumulative_state) must stay within the block gas limit.
   *
   * @param txGasLimit the gas limit of the candidate transaction
   * @param cumulativeRegularGas cumulative regular gas used so far
   * @param cumulativeStateGas cumulative state gas used so far
   * @param blockGasLimit the block gas limit
   * @return true if the block has capacity for this transaction
   */
  default boolean hasBlockCapacity(
      final long txGasLimit,
      final long cumulativeRegularGas,
      final long cumulativeStateGas,
      final long blockGasLimit) {
    return txGasLimit <= blockGasLimit - cumulativeRegularGas;
  }

  /**
   * Calculate the effective gas used for occupancy and fullness checks. For 1D gas, this is just
   * the regular gas. For 2D gas (EIP-8037), this is max(regular, state).
   *
   * @param cumulativeRegularGas cumulative regular gas used
   * @param cumulativeStateGas cumulative state gas used
   * @return the effective gas used
   */
  default long effectiveGasUsed(final long cumulativeRegularGas, final long cumulativeStateGas) {
    return cumulativeRegularGas;
  }

  /**
   * Frontier through BPO5: Uses post-refund gas (gasLimit - gasRemaining). This is the traditional
   * Ethereum behavior where refunds reduce the effective gas used for block limit purposes.
   */
  BlockGasAccountingStrategy FRONTIER = (tx, result) -> tx.getGasLimit() - result.getGasRemaining();

  /**
   * Amsterdam (EIP-7778 + EIP-8037): Uses pre-refund gas split into regular and state dimensions.
   *
   * <p>EIP-7778: Block gas is calculated pre-refund (estimateGasUsedByTransaction), preventing
   * block gas limit circumvention through refund credits.
   *
   * <p>EIP-8037: Gas is split into regular and state portions. Regular gas =
   * estimateGasUsedByTransaction - stateGasUsed. Block gas_metered = max(cumulative_regular,
   * cumulative_state).
   */
  BlockGasAccountingStrategy AMSTERDAM =
      new BlockGasAccountingStrategy() {
        @Override
        public long calculateTransactionRegularGas(
            final Transaction transaction, final TransactionProcessingResult result) {
          return result.getEstimateGasUsedByTransaction() - result.getStateGasUsed();
        }

        @Override
        public boolean hasBlockCapacity(
            final long txGasLimit,
            final long cumulativeRegularGas,
            final long cumulativeStateGas,
            final long blockGasLimit) {
          // EIP-8037: Only check regular gas dimension. State gas is validated at block level
          // via effectiveGasUsed() = max(regular, state) after transaction execution.
          // The tx gas limit is not an accurate proxy for state gas usage, so checking it
          // against remaining state capacity would reject valid blocks where individual txs
          // exceed the state gas budget but the block total is within limits.
          final long remainingRegular = Math.max(0, blockGasLimit - cumulativeRegularGas);
          return txGasLimit <= remainingRegular;
        }

        @Override
        public long effectiveGasUsed(
            final long cumulativeRegularGas, final long cumulativeStateGas) {
          return Math.max(cumulativeRegularGas, cumulativeStateGas);
        }
      };

  /**
   * Calculates the gas to be used in transaction receipts. This is always the standard post-refund
   * calculation (gasLimit - gasRemaining), regardless of the block gas accounting strategy.
   *
   * <p>Receipt gas is protocol-invariant: it always reflects the actual gas charged to the user
   * after refunds are applied. This differs from block gas accounting which may use pre-refund
   * values (EIP-7778) for block limit enforcement.
   *
   * @param transaction the transaction being processed
   * @param result the transaction processing result
   * @return the gas amount to record in the receipt
   */
  static long calculateReceiptGas(
      final Transaction transaction, final TransactionProcessingResult result) {
    return transaction.getGasLimit() - result.getGasRemaining();
  }
}
