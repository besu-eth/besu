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

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockReadCounts;
import org.hyperledger.besu.ethereum.mainnet.slowblock.SlowBlockTxRecorder;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;

import java.util.Objects;

public final class ParallelizedTransactionContext {
  private final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator;
  private final TransactionProcessingResult transactionProcessingResult;
  private final boolean isMiningBeneficiaryTouchedPreRewardByTransaction;
  private final Wei miningBeneficiaryReward;
  private final SlowBlockTxRecorder slowBlockRecorder;
  private final SlowBlockReadCounts slowBlockReads;
  private final long slowBlockExecutionNanos;

  public ParallelizedTransactionContext(
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final TransactionProcessingResult transactionProcessingResult,
      final boolean isMiningBeneficiaryTouchedPreRewardByTransaction,
      final Wei miningBeneficiaryReward) {
    this(
        transactionAccumulator,
        transactionProcessingResult,
        isMiningBeneficiaryTouchedPreRewardByTransaction,
        miningBeneficiaryReward,
        null,
        null,
        0L);
  }

  public ParallelizedTransactionContext(
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final TransactionProcessingResult transactionProcessingResult,
      final boolean isMiningBeneficiaryTouchedPreRewardByTransaction,
      final Wei miningBeneficiaryReward,
      final SlowBlockTxRecorder slowBlockRecorder,
      final SlowBlockReadCounts slowBlockReads,
      final long slowBlockExecutionNanos) {
    this.transactionAccumulator = transactionAccumulator;
    this.transactionProcessingResult = transactionProcessingResult;
    this.isMiningBeneficiaryTouchedPreRewardByTransaction =
        isMiningBeneficiaryTouchedPreRewardByTransaction;
    this.miningBeneficiaryReward = miningBeneficiaryReward;
    this.slowBlockRecorder = slowBlockRecorder;
    this.slowBlockReads = slowBlockReads;
    this.slowBlockExecutionNanos = slowBlockExecutionNanos;
  }

  public PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator() {
    return transactionAccumulator;
  }

  /**
   * The EVM counters recorded while this transaction ran in the background, or null when slow-block
   * tracing is disabled. Merged into the block aggregate only if this result is used.
   *
   * @return the per-transaction recorder, or null
   */
  public SlowBlockTxRecorder slowBlockRecorder() {
    return slowBlockRecorder;
  }

  /**
   * The logical state reads made while this transaction ran in the background, or null when
   * slow-block tracing is disabled.
   *
   * @return the per-transaction read counts, or null
   */
  public SlowBlockReadCounts slowBlockReads() {
    return slowBlockReads;
  }

  /**
   * Time this transaction spent executing in the background, excluding time spent queued.
   *
   * @return elapsed nanoseconds
   */
  public long slowBlockExecutionNanos() {
    return slowBlockExecutionNanos;
  }

  public TransactionProcessingResult transactionProcessingResult() {
    return transactionProcessingResult;
  }

  public boolean isMiningBeneficiaryTouchedPreRewardByTransaction() {
    return isMiningBeneficiaryTouchedPreRewardByTransaction;
  }

  public Wei miningBeneficiaryReward() {
    return miningBeneficiaryReward;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) return true;
    if (obj == null || obj.getClass() != this.getClass()) return false;
    var that = (ParallelizedTransactionContext) obj;
    return Objects.equals(this.transactionAccumulator, that.transactionAccumulator)
        && Objects.equals(this.transactionProcessingResult, that.transactionProcessingResult)
        && this.isMiningBeneficiaryTouchedPreRewardByTransaction
            == that.isMiningBeneficiaryTouchedPreRewardByTransaction
        && Objects.equals(this.miningBeneficiaryReward, that.miningBeneficiaryReward);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        transactionAccumulator,
        transactionProcessingResult,
        isMiningBeneficiaryTouchedPreRewardByTransaction,
        miningBeneficiaryReward);
  }

  @Override
  public String toString() {
    return "ParallelizedTransactionContext["
        + "transactionAccumulator="
        + transactionAccumulator
        + ", "
        + "transactionProcessingResult="
        + transactionProcessingResult
        + ", "
        + "isMiningBeneficiaryTouchedPreRewardByTransaction="
        + isMiningBeneficiaryTouchedPreRewardByTransaction
        + ", "
        + "miningBeneficiaryReward="
        + miningBeneficiaryReward
        + ']';
  }

  public static class Builder {
    private PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator;
    private TransactionProcessingResult transactionProcessingResult;
    private boolean isMiningBeneficiaryTouchedPreRewardByTransaction;
    private Wei miningBeneficiaryReward = Wei.ZERO;
    private SlowBlockTxRecorder slowBlockRecorder;
    private SlowBlockReadCounts slowBlockReads;
    private long slowBlockExecutionNanos;

    public Builder transactionAccumulator(
        final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator) {
      this.transactionAccumulator = transactionAccumulator;
      return this;
    }

    public Builder transactionProcessingResult(
        final TransactionProcessingResult transactionProcessingResult) {
      this.transactionProcessingResult = transactionProcessingResult;
      return this;
    }

    public Builder isMiningBeneficiaryTouchedPreRewardByTransaction(
        final boolean isMiningBeneficiaryTouchedPreRewardByTransaction) {
      this.isMiningBeneficiaryTouchedPreRewardByTransaction =
          isMiningBeneficiaryTouchedPreRewardByTransaction;
      return this;
    }

    public Builder miningBeneficiaryReward(final Wei miningBeneficiaryReward) {
      this.miningBeneficiaryReward = miningBeneficiaryReward;
      return this;
    }

    public Builder slowBlockRecorder(final SlowBlockTxRecorder slowBlockRecorder) {
      this.slowBlockRecorder = slowBlockRecorder;
      return this;
    }

    public Builder slowBlockReads(final SlowBlockReadCounts slowBlockReads) {
      this.slowBlockReads = slowBlockReads;
      return this;
    }

    public Builder slowBlockExecutionNanos(final long slowBlockExecutionNanos) {
      this.slowBlockExecutionNanos = slowBlockExecutionNanos;
      return this;
    }

    public ParallelizedTransactionContext build() {
      return new ParallelizedTransactionContext(
          transactionAccumulator,
          transactionProcessingResult,
          isMiningBeneficiaryTouchedPreRewardByTransaction,
          miningBeneficiaryReward,
          slowBlockRecorder,
          slowBlockReads,
          slowBlockExecutionNanos);
    }
  }
}
