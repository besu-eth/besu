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
import org.hyperledger.besu.ethereum.mainnet.ExecutionStats;
import org.hyperledger.besu.ethereum.processing.TransactionProcessingResult;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Objects;
import java.util.Optional;

public final class ParallelizedTransactionContext {
  private final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator;
  private final TransactionProcessingResult transactionProcessingResult;
  private final boolean isMiningBeneficiaryTouchedPreRewardByTransaction;
  private final Wei miningBeneficiaryReward;
  private final Optional<OperationTracer> backgroundTracer;
  private final Optional<ExecutionStats> workerExecutionStats;

  public ParallelizedTransactionContext(
      final PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator,
      final TransactionProcessingResult transactionProcessingResult,
      final boolean isMiningBeneficiaryTouchedPreRewardByTransaction,
      final Wei miningBeneficiaryReward,
      final Optional<OperationTracer> backgroundTracer,
      final Optional<ExecutionStats> workerExecutionStats) {
    this.transactionAccumulator = transactionAccumulator;
    this.transactionProcessingResult = transactionProcessingResult;
    this.isMiningBeneficiaryTouchedPreRewardByTransaction =
        isMiningBeneficiaryTouchedPreRewardByTransaction;
    this.miningBeneficiaryReward = miningBeneficiaryReward;
    this.backgroundTracer = backgroundTracer;
    this.workerExecutionStats = workerExecutionStats;
  }

  public PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator() {
    return transactionAccumulator;
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

  public Optional<OperationTracer> backgroundTracer() {
    return backgroundTracer;
  }

  public Optional<ExecutionStats> workerExecutionStats() {
    return workerExecutionStats;
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
        && Objects.equals(this.miningBeneficiaryReward, that.miningBeneficiaryReward)
        && Objects.equals(this.backgroundTracer, that.backgroundTracer)
        && Objects.equals(this.workerExecutionStats, that.workerExecutionStats);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        transactionAccumulator,
        transactionProcessingResult,
        isMiningBeneficiaryTouchedPreRewardByTransaction,
        miningBeneficiaryReward,
        backgroundTracer,
        workerExecutionStats);
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
        + ", "
        + "backgroundTracer="
        + backgroundTracer
        + ", "
        + "workerExecutionStats="
        + workerExecutionStats
        + ']';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private PathBasedWorldStateUpdateAccumulator<?> transactionAccumulator;
    private TransactionProcessingResult transactionProcessingResult;
    private boolean isMiningBeneficiaryTouchedPreRewardByTransaction;
    private Wei miningBeneficiaryReward = Wei.ZERO;
    private Optional<OperationTracer> backgroundTracer = Optional.empty();
    private Optional<ExecutionStats> workerExecutionStats = Optional.empty();

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

    public Builder backgroundTracer(final OperationTracer backgroundTracer) {
      this.backgroundTracer = Optional.ofNullable(backgroundTracer);
      return this;
    }

    public Builder workerExecutionStats(final ExecutionStats workerExecutionStats) {
      this.workerExecutionStats = Optional.ofNullable(workerExecutionStats);
      return this;
    }

    public ParallelizedTransactionContext build() {
      return new ParallelizedTransactionContext(
          transactionAccumulator,
          transactionProcessingResult,
          isMiningBeneficiaryTouchedPreRewardByTransaction,
          miningBeneficiaryReward,
          backgroundTracer,
          workerExecutionStats);
    }
  }
}
