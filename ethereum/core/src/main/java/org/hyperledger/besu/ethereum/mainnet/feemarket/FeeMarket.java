/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.config.BlobSchedule;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.TransactionPriceCalculator;

import java.math.BigInteger;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

public interface FeeMarket {

  /**
   * Indicates whether the fee market implements the base fee mechanism (e.g., EIP-1559).
   *
   * @return {@code true} if base fee is implemented, otherwise {@code false}
   */
  default boolean implementsBaseFee() {
    return false;
  }

  /**
   * Indicates whether the fee market implements a blob fee mechanism.
   *
   * @return {@code true} if the fee market implements a blob fee, {@code false} otherwise.
   */
  default boolean implementsBlobFee() {
    return false;
  }

  /**
   * Retrieves the transaction price calculator associated with this fee market.
   *
   * @return the {@link TransactionPriceCalculator} instance.
   */
  TransactionPriceCalculator getTransactionPriceCalculator();

  /**
   * Determines whether a given transaction satisfies the floor transaction fee requirements.
   *
   * @param txn the {@link Transaction} to evaluate.
   * @return {@code true} if the transaction satisfies the floor fee, {@code false} otherwise.
   */
  boolean satisfiesFloorTxFee(Transaction txn);

  /**
   * Creates a London fee market with the specified fork block number.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @return a {@link BaseFeeMarket} instance for the London fork.
   */
  @VisibleForTesting
  static BaseFeeMarket london(final long londonForkBlockNumber) {
    return london(londonForkBlockNumber, Optional.empty());
  }

  /**
   * Creates a London fee market with the specified fork block number and optional base fee
   * override.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param baseFeePerGasOverride an optional override for the base fee per gas.
   * @param ignored a {@link BlobSchedule} parameter that is ignored in this context.
   * @return a {@link BaseFeeMarket} instance for the London fork.
   */
  static BaseFeeMarket london(
      final long londonForkBlockNumber,
      final Optional<Wei> baseFeePerGasOverride,
      final BlobSchedule ignored) {
    return london(londonForkBlockNumber, baseFeePerGasOverride);
  }

  /**
   * Creates a London fee market with the specified fork block number and optional base fee
   * override.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param baseFeePerGasOverride an optional override for the base fee per gas.
   * @return a {@link BaseFeeMarket} instance for the London fork.
   */
  @VisibleForTesting
  static BaseFeeMarket london(
      final long londonForkBlockNumber, final Optional<Wei> baseFeePerGasOverride) {
    return new LondonFeeMarket(londonForkBlockNumber, baseFeePerGasOverride);
  }

  /**
   * Creates a Blob fee market with default Cancun blob schedule parameters.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param baseFeePerGasOverride an optional override for the base fee per gas.
   * @return a {@link BaseFeeMarket} instance for the Cancun fork with default blob schedule.
   */
  @VisibleForTesting
  static BaseFeeMarket cancunDefault(
      final long londonForkBlockNumber, final Optional<Wei> baseFeePerGasOverride) {
    return new BlobFeeMarket(
        londonForkBlockNumber,
        baseFeePerGasOverride,
        BlobSchedule.CANCUN_DEFAULT.getBaseFeeUpdateFraction());
  }

  /**
   * Creates a Blob fee market with the specified blob schedule parameters.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param baseFeePerGasOverride an optional override for the base fee per gas.
   * @param blobSchedule the {@link BlobSchedule} defining blob-related parameters.
   * @return a {@link BaseFeeMarket} instance for the Cancun fork.
   */
  static BaseFeeMarket cancun(
      final long londonForkBlockNumber,
      final Optional<Wei> baseFeePerGasOverride,
      final BlobSchedule blobSchedule) {
    return new BlobFeeMarket(
        londonForkBlockNumber, baseFeePerGasOverride, blobSchedule.getBaseFeeUpdateFraction());
  }

  /**
   * Creates a fee market with a zero base fee.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @return a {@link BaseFeeMarket} instance with a zero base fee.
   */
  static BaseFeeMarket zeroBaseFee(final long londonForkBlockNumber) {
    return new ZeroBaseFeeMarket(londonForkBlockNumber);
  }

  /**
   * Creates a fee market with a zero blob fee.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @return a {@link BaseFeeMarket} instance with a zero blob fee.
   */
  static BaseFeeMarket zeroBlobFee(final long londonForkBlockNumber) {
    return new ZeroBlobFeeMarket(londonForkBlockNumber);
  }

  /**
   * Creates a fee market with a fixed base fee.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param fixedBaseFee the fixed base fee to use.
   * @return a {@link BaseFeeMarket} instance with a fixed base fee.
   */
  static BaseFeeMarket fixedBaseFee(final long londonForkBlockNumber, final Wei fixedBaseFee) {
    return new FixedBaseFeeMarket(londonForkBlockNumber, fixedBaseFee);
  }

  /**
   * Creates a fee market with a fixed base fee and blob fee support.
   *
   * @param londonForkBlockNumber the block number at which the London fork activates.
   * @param fixedBaseFee the fixed base fee to use.
   * @return a {@link BaseFeeMarket} instance with a fixed base fee and blob fee support.
   */
  static BaseFeeMarket fixedBlobFee(final long londonForkBlockNumber, final Wei fixedBaseFee) {
    return new FixedBlobFeeMarket(londonForkBlockNumber, fixedBaseFee);
  }

  /**
   * Creates a legacy fee market.
   *
   * @return a {@link FeeMarket} instance for legacy transactions.
   */
  static FeeMarket legacy() {
    return new LegacyFeeMarket();
  }

  /**
   * Calculates the blob gas price per gas based on the excess blob gas.
   *
   * @param excessBlobGas the excess blob gas.
   * @return the blob gas price per gas as a {@link Wei} value.
   */
  default Wei blobGasPricePerGas(final BlobGas excessBlobGas) {
    return Wei.ZERO;
  }

  /**
   * Returns the base fee update fraction. Only for blobs.
   *
   * @return the base fee update fraction.
   */
  default BigInteger getBaseFeeUpdateFraction() {
    return BigInteger.ZERO;
  }
}
