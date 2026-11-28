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
package org.hyperledger.besu.consensus.common.bft;

import org.hyperledger.besu.ethereum.GasLimitCalculator;

import java.util.Objects;

/**
 * BFT {@code pertxgaslimit} override of the per-transaction gas caps.
 *
 * <p>A configured value of {@code 0} (or any non-positive value) means "no BFT override": both
 * {@link #transactionGasLimitCap()} and {@link #transactionIntrinsicGasLimitCap()} stay on the fork
 * (Osaka EIP-7825 / Amsterdam EIP-8037). Genesis parsing already rejects a negative {@code
 * pertxgaslimit}; the {@code > 0} check here is defensive. A positive value {@code N} sets both
 * caps to {@code N}, so operators can raise the limit above those Ethereum defaults (for example
 * the QBFT soak {@code pertxgaslimit} of 40M above Osaka's 2^24) or lower it.
 */
public final class TransactionGasLimitOverrideGasLimitCalculator implements GasLimitCalculator {

  private final GasLimitCalculator delegate;
  private final long transactionGasLimitCap;

  /**
   * Instantiates a new TransactionGasLimitOverrideGasLimitCalculator.
   *
   * @param delegate the delegate gas limit calculator
   * @param transactionGasLimitCap the BFT per-transaction gas limit; {@code 0} disables the BFT
   *     override
   */
  public TransactionGasLimitOverrideGasLimitCalculator(
      final GasLimitCalculator delegate, final long transactionGasLimitCap) {
    this.delegate = Objects.requireNonNull(delegate);
    this.transactionGasLimitCap = transactionGasLimitCap;
  }

  @Override
  public long nextGasLimit(
      final long currentGasLimit, final long targetGasLimit, final long newBlockNumber) {
    return delegate.nextGasLimit(currentGasLimit, targetGasLimit, newBlockNumber);
  }

  @Override
  public long currentBlobGasLimit() {
    return delegate.currentBlobGasLimit();
  }

  @Override
  public long getTargetBlobGasPerBlock() {
    return delegate.getTargetBlobGasPerBlock();
  }

  @Override
  public long computeExcessBlobGas(
      final long parentExcessBlobGas, final long blobGasUsed, final long parentBaseFeePerGas) {
    return delegate.computeExcessBlobGas(parentExcessBlobGas, blobGasUsed, parentBaseFeePerGas);
  }

  @Override
  public long transactionGasLimitCap() {
    if (transactionGasLimitCap > 0L) {
      return transactionGasLimitCap;
    }
    return delegate.transactionGasLimitCap();
  }

  @Override
  public long transactionIntrinsicGasLimitCap() {
    if (transactionGasLimitCap > 0L) {
      // Same N for tx.gas and intrinsic/storage budget (matthew1001 / #11492 review).
      return transactionGasLimitCap;
    }
    return delegate.transactionIntrinsicGasLimitCap();
  }

  @Override
  public long transactionBlobGasLimitCap() {
    return delegate.transactionBlobGasLimitCap();
  }

  @Override
  public long blockBuilderBlobGasLimit() {
    return delegate.blockBuilderBlobGasLimit();
  }
}
