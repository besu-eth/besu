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

import org.hyperledger.besu.config.GenesisConfig;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.feemarket.TransactionPriceCalculator;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.units.bigints.UInt256s;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LondonFeeMarket implements BaseFeeMarket {
  private static final Logger LOG = LoggerFactory.getLogger(LondonFeeMarket.class);

  static final Wei DEFAULT_BASEFEE_INITIAL_VALUE = GenesisConfig.BASEFEE_AT_GENESIS_DEFAULT_VALUE;
  static final long DEFAULT_BASEFEE_MAX_CHANGE_DENOMINATOR = 8L;
  static final long DEFAULT_SLACK_COEFFICIENT = 2L;

  private static final Wei DEFAULT_BASEFEE_FLOOR = Wei.of(7L);

  protected final Wei baseFeeInitialValue;
  private final long londonForkBlockNumber;
  private final TransactionPriceCalculator txPriceCalculator;
  private final Wei baseFeeFloor;

  LondonFeeMarket(final long londonForkBlockNumber, final Optional<Wei> baseFeePerGasOverride) {
    this(TransactionPriceCalculator.eip1559(), londonForkBlockNumber, baseFeePerGasOverride);
  }

  LondonFeeMarket(
      final TransactionPriceCalculator txPriceCalculator,
      final long londonForkBlockNumber,
      final Optional<Wei> baseFeePerGasOverride) {
    this.txPriceCalculator = txPriceCalculator;
    this.londonForkBlockNumber = londonForkBlockNumber;
    this.baseFeeInitialValue = baseFeePerGasOverride.orElse(DEFAULT_BASEFEE_INITIAL_VALUE);
    this.baseFeeFloor = baseFeeInitialValue.isZero() ? Wei.ZERO : DEFAULT_BASEFEE_FLOOR;
  }

  @Override
  public long getBasefeeMaxChangeDenominator() {
    return DEFAULT_BASEFEE_MAX_CHANGE_DENOMINATOR;
  }

  @Override
  public Wei getInitialBasefee() {
    return baseFeeInitialValue;
  }

  @Override
  public long getSlackCoefficient() {
    return DEFAULT_SLACK_COEFFICIENT;
  }

  @Override
  public TransactionPriceCalculator getTransactionPriceCalculator() {
    return txPriceCalculator;
  }

  @Override
  public boolean satisfiesFloorTxFee(final Transaction txn) {
    // ensure effective baseFee is at least above floor
    return txn.getGasPrice()
        .map(Optional::of)
        .orElse(txn.getMaxFeePerGas())
        .filter(fee -> fee.greaterOrEqualThan(baseFeeFloor))
        .isPresent();
  }

  @Override
  public Wei computeBaseFee(
      final long blockNumber,
      final long timestamp,
      final long slotNumber,
      final BlockHeader parentBlockHeader) {
    return computeBaseFee(blockNumber, parentBlockHeader);
  }

  @Override
  public Wei predictNextBaseFee(
      final BlockHeader headBlockHeader,
      final Supplier<Optional<BlockHeader>> headParentBlockHeaderSupplier) {
    // the base fee does not depend on the spacing of the blocks, so the parent of the head is not
    // needed
    return computeBaseFee(headBlockHeader.getNumber() + 1, headBlockHeader);
  }

  private Wei computeBaseFee(final long blockNumber, final BlockHeader parentBlockHeader) {
    if (londonForkBlockNumber == blockNumber) {
      return getInitialBasefee();
    }

    final Wei parentBaseFee = parentBlockHeader.getBaseFee().orElse(Wei.ZERO);
    final long parentBlockGasUsed = parentBlockHeader.getGasUsed();
    final long targetGasUsed = targetGasUsed(parentBlockHeader);

    long gasDelta;
    Wei feeDelta, baseFee;
    if (parentBlockGasUsed == targetGasUsed) {
      return parentBaseFee;
    } else if (parentBlockGasUsed > targetGasUsed) {
      gasDelta = parentBlockGasUsed - targetGasUsed;
      final long denominator = getBasefeeMaxChangeDenominator();
      feeDelta =
          UInt256s.max(
              parentBaseFee.multiply(gasDelta).divide(targetGasUsed).divide(denominator), Wei.ONE);
      baseFee = parentBaseFee.add(feeDelta);
    } else {
      gasDelta = targetGasUsed - parentBlockGasUsed;
      final long denominator = getBasefeeMaxChangeDenominator();
      feeDelta = parentBaseFee.multiply(gasDelta).divide(targetGasUsed).divide(denominator);
      baseFee = parentBaseFee.subtract(feeDelta);
    }
    LOG.trace(
        "block #{} parentBaseFee: {} parentGasUsed: {} parentGasTarget: {} baseFee: {}",
        blockNumber,
        parentBaseFee,
        parentBlockGasUsed,
        targetGasUsed,
        baseFee);
    return baseFee;
  }

  @Override
  public ValidationMode baseFeeValidationMode(final long blockNumber) {
    return londonForkBlockNumber == blockNumber ? ValidationMode.INITIAL : ValidationMode.ONGOING;
  }

  @Override
  public ValidationMode gasLimitValidationMode(final long blockNumber) {
    return londonForkBlockNumber == blockNumber ? ValidationMode.INITIAL : ValidationMode.ONGOING;
  }

  @Override
  public boolean isBeforeForkBlock(final long blockNumber) {
    return londonForkBlockNumber > blockNumber;
  }
}
