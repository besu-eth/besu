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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;

/**
 * The EIP-8141 fee budget of a frame transaction. Its {@code max_fee} caps the total wei paid for
 * all of its execution, state and blob gas; it must cover {@code max_gas} at the base fee plus the
 * blob gas at the blob base fee, and what is left can back a tip.
 *
 * <p>The arithmetic runs on {@link BigInteger}, because {@code max_priority_fee_per_gas * max_gas}
 * can exceed {@code 2^256}. Every result fits in {@link Wei} once the budget covers the required
 * fee, since none exceeds {@code max_fee}.
 */
public final class FrameTransactionFees {

  /**
   * What a frame transaction's payer escrows when a frame approves payment, and the price per gas
   * that {@code GASPRICE} reports.
   *
   * @param maxCost the escrow: the required fee plus the largest tip the rest of the budget allows
   * @param gasPrice the base fee plus the tip per gas the rest of the budget can pay on every unit
   *     of {@code max_gas}
   */
  public record Escrow(Wei maxCost, Wei gasPrice) {}

  /**
   * How the escrow is disbursed once the transaction has run. The burned base fee is the rest.
   *
   * @param priorityFeePaid the tip credited to the coinbase
   * @param payerRefund the unspent escrow returned to the payer
   */
  public record Disbursement(Wei priorityFeePaid, Wei payerRefund) {}

  private FrameTransactionFees() {}

  /**
   * The least {@code max_fee} that admits the transaction: {@code max_gas * base_fee + blob_gas *
   * blob_base_fee}.
   *
   * @param maxGas the transaction's gas limit, {@code max_gas}
   * @param baseFee the block's base fee per gas
   * @param blobGas the transaction's total blob gas
   * @param blobGasPrice the block's blob base fee per blob gas
   * @return the required max fee, in wei
   */
  public static BigInteger requiredMaxFee(
      final long maxGas, final Wei baseFee, final long blobGas, final Wei blobGasPrice) {
    return baseFeeCost(maxGas, baseFee, blobGas, blobGasPrice);
  }

  /**
   * The escrow and gas price of a frame transaction at a block's base fees.
   *
   * <p>A budget below the required fee leaves no room for a tip, so the escrow is the required fee.
   * Admission rejects such a transaction; only callers that skip the fee check reach this case.
   *
   * @param maxFee the transaction's {@code max_fee}
   * @param maxPriorityFeePerGas the transaction's {@code max_priority_fee_per_gas}
   * @param maxGas the transaction's gas limit, {@code max_gas}
   * @param baseFee the block's base fee per gas
   * @param blobGas the transaction's total blob gas
   * @param blobGasPrice the block's blob base fee per blob gas
   * @return the escrow and the gas price
   */
  public static Escrow escrow(
      final Wei maxFee,
      final Wei maxPriorityFeePerGas,
      final long maxGas,
      final Wei baseFee,
      final long blobGas,
      final Wei blobGasPrice) {
    final BigInteger required = requiredMaxFee(maxGas, baseFee, blobGas, blobGasPrice);
    final BigInteger maxPriorityFee =
        maxPriorityFee(maxFee, maxPriorityFeePerGas, maxGas, required);
    return new Escrow(
        Wei.of(required.add(maxPriorityFee)),
        Wei.of(baseFee.getAsBigInteger().add(maxPriorityFee.divide(BigInteger.valueOf(maxGas)))));
  }

  /**
   * The tip per gas a frame transaction pays on every unit of {@code max_gas}: {@link
   * Escrow#gasPrice()} less the base fee.
   *
   * @param maxFee the transaction's {@code max_fee}
   * @param maxPriorityFeePerGas the transaction's {@code max_priority_fee_per_gas}
   * @param maxGas the transaction's gas limit, {@code max_gas}
   * @param baseFee the block's base fee per gas
   * @param blobGas the transaction's total blob gas
   * @param blobGasPrice the block's blob base fee per blob gas
   * @return the tip per gas, in wei
   */
  public static Wei tipPerGas(
      final Wei maxFee,
      final Wei maxPriorityFeePerGas,
      final long maxGas,
      final Wei baseFee,
      final long blobGas,
      final Wei blobGasPrice) {
    final BigInteger required = requiredMaxFee(maxGas, baseFee, blobGas, blobGasPrice);
    return Wei.of(
        maxPriorityFee(maxFee, maxPriorityFeePerGas, maxGas, required)
            .divide(BigInteger.valueOf(maxGas)));
  }

  /**
   * Disburses the escrow: the gas used at the base fee and the blob gas at the blob base fee are
   * burned, the coinbase receives the tip per gas on the gas used, capped by what the escrow holds
   * after the burn, and the payer gets back the rest.
   *
   * @param maxCost the escrow, {@link Escrow#maxCost()}
   * @param maxPriorityFeePerGas the transaction's {@code max_priority_fee_per_gas}
   * @param gasUsed the gas the transaction used, at most {@code max_gas}
   * @param baseFee the block's base fee per gas
   * @param blobGas the transaction's total blob gas
   * @param blobGasPrice the block's blob base fee per blob gas
   * @return the coinbase's tip and the payer's refund
   */
  public static Disbursement disburse(
      final Wei maxCost,
      final Wei maxPriorityFeePerGas,
      final long gasUsed,
      final Wei baseFee,
      final long blobGas,
      final Wei blobGasPrice) {
    final BigInteger left =
        maxCost.getAsBigInteger().subtract(baseFeeCost(gasUsed, baseFee, blobGas, blobGasPrice));
    final BigInteger priorityFeePaid =
        maxPriorityFeePerGas.getAsBigInteger().multiply(BigInteger.valueOf(gasUsed)).min(left);
    return new Disbursement(Wei.of(priorityFeePaid), Wei.of(left.subtract(priorityFeePaid)));
  }

  private static BigInteger baseFeeCost(
      final long gas, final Wei baseFee, final long blobGas, final Wei blobGasPrice) {
    return baseFee
        .getAsBigInteger()
        .multiply(BigInteger.valueOf(gas))
        .add(blobGasPrice.getAsBigInteger().multiply(BigInteger.valueOf(blobGas)));
  }

  private static BigInteger maxPriorityFee(
      final Wei maxFee,
      final Wei maxPriorityFeePerGas,
      final long maxGas,
      final BigInteger required) {
    final BigInteger budgetLeft = maxFee.getAsBigInteger().subtract(required).max(BigInteger.ZERO);
    return maxPriorityFeePerGas
        .getAsBigInteger()
        .multiply(BigInteger.valueOf(maxGas))
        .min(budgetLeft);
  }
}
