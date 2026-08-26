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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Wei;

import java.math.BigInteger;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobFeeMarket extends LondonFeeMarket {
  private static final Logger LOG = LoggerFactory.getLogger(BlobFeeMarket.class);
  protected static final BigInteger BLOB_GAS_PRICE = BigInteger.ONE;
  public static final Wei MIN_BLOB_GASPRICE = Wei.ONE;

  private final BigInteger baseFeeUpdateFraction;

  BlobFeeMarket(
      final long londonForkBlockNumber,
      final Optional<Wei> baseFeePerGasOverride,
      final long baseFeeUpdateFraction) {
    super(londonForkBlockNumber, baseFeePerGasOverride);
    this.baseFeeUpdateFraction = BigInteger.valueOf(baseFeeUpdateFraction);
  }

  @Override
  public boolean implementsBlobFee() {
    return true;
  }

  @Override
  public Wei blobGasPricePerGas(final BlobGas excessBlobGas) {
    final var blobGasPrice =
        Wei.of(
            fakeExponential(
                BLOB_GAS_PRICE, excessBlobGas.toBigInteger(), getBaseFeeUpdateFraction()));
    LOG.atTrace()
        .setMessage("parentExcessBlobGas: {} blobGasPrice: {}")
        .addArgument(excessBlobGas::toShortHexString)
        .addArgument(blobGasPrice::toHexString)
        .log();

    return blobGasPrice;
  }

  protected BigInteger fakeExponential(
      final BigInteger factor, final BigInteger numerator, final BigInteger denominator) {
    int i = 1;
    BigInteger output = BigInteger.ZERO;
    BigInteger numeratorAccumulator = factor.multiply(denominator);
    while (numeratorAccumulator.signum() > 0) {
      output = output.add(numeratorAccumulator);
      numeratorAccumulator =
          (numeratorAccumulator.multiply(numerator))
              .divide(denominator.multiply(BigInteger.valueOf(i)));
      ++i;
    }
    return output.divide(denominator);
  }

  @Override
  public BigInteger getBaseFeeUpdateFraction() {
    return baseFeeUpdateFraction;
  }
}
