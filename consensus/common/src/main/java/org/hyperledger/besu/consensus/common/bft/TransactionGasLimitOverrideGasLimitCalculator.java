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
 * A delegating gas limit calculator that can lower the per-transaction gas limit cap for BFT.
 *
 * <p>The fork's {@link #transactionIntrinsicGasLimitCap()} is always taken from the delegate, so
 * Amsterdam's EIP-8037 intrinsic-gas cap stays in force. An override of {@code 0} (or {@link
 * Long#MAX_VALUE}) leaves {@link #transactionGasLimitCap()} on the delegate as well; any other
 * override is clamped to the delegate when the fork has a consensus-level cap.
 */
public final class TransactionGasLimitOverrideGasLimitCalculator implements GasLimitCalculator {

  private final GasLimitCalculator delegate;
  private final long transactionGasLimitCap;

  /**
   * Instantiates a new TransactionGasLimitOverrideGasLimitCalculator.
   *
   * @param delegate the delegate gas limit calculator
   * @param transactionGasLimitCap the transaction gas limit cap to apply
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
    final long delegateCap = delegate.transactionGasLimitCap();
    // 0 (and the historical Long.MAX_VALUE sentinel for "unlimited") means "do not
    // override" — keep the fork's consensus-level cap (e.g. Amsterdam 2^32-1, Osaka 2^24).
    if (transactionGasLimitCap == 0L || transactionGasLimitCap == Long.MAX_VALUE) {
      return delegateCap;
    }
    // Never raise a configured override above a consensus-level cap on the fork.
    if (delegateCap != Long.MAX_VALUE) {
      return Math.min(transactionGasLimitCap, delegateCap);
    }
    return transactionGasLimitCap;
  }

  @Override
  public long transactionIntrinsicGasLimitCap() {
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
