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
package org.hyperledger.besu.ethereum.processing;

import org.hyperledger.besu.datatypes.Log;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.PartialBlockAccessView;
import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;
import org.hyperledger.besu.ethereum.trie.pathbased.common.worldview.accumulator.PathBasedWorldStateUpdateAccumulator;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class TransactionProcessingResult
    implements org.hyperledger.besu.plugin.data.TransactionProcessingResult {

  /** The status of the transaction after being processed. */
  public enum Status {

    /** The transaction was invalid for processing. */
    INVALID,

    /** The transaction was successfully processed. */
    SUCCESSFUL,

    /** The transaction failed to be completely processed. */
    FAILED
  }

  private final Status status;

  private final long estimateGasUsedByTransaction;

  private final long gasRemaining;

  private final long gasSpent;

  private final long stateGasUsed;

  private final List<Log> logs;

  private final Bytes output;

  private Optional<Boolean> isProcessedInParallel = Optional.empty();

  private final ValidationResult<TransactionInvalidReason> validationResult;
  private final Optional<Bytes> revertReason;

  public PathBasedWorldStateUpdateAccumulator<?> accumulator;
  private final Optional<ExceptionalHaltReason> exceptionalHaltReason;
  private final Optional<PartialBlockAccessView> partialBlockAccessView;

  public static TransactionProcessingResult invalid(
      final ValidationResult<TransactionInvalidReason> validationResult) {
    return new TransactionProcessingResult(
        Status.INVALID,
        List.of(),
        -1,
        -1,
        -1,
        0,
        Bytes.EMPTY,
        validationResult,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  public static TransactionProcessingResult failed(
      final long gasUsedByTransaction,
      final long gasRemaining,
      final long gasSpent,
      final long stateGasUsed,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<ExceptionalHaltReason> exceptionalHaltReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    return new TransactionProcessingResult(
        Status.FAILED,
        List.of(),
        gasUsedByTransaction,
        gasRemaining,
        gasSpent,
        stateGasUsed,
        Bytes.EMPTY,
        validationResult,
        revertReason,
        exceptionalHaltReason,
        partialBlockAccessView);
  }

  /**
   * Factory method for failed transactions where gasSpent equals gasUsedByTransaction. This is the
   * standard behavior for pre-Amsterdam forks where block gas accounting uses post-refund gas.
   */
  public static TransactionProcessingResult failed(
      final long gasUsedByTransaction,
      final long gasRemaining,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<ExceptionalHaltReason> exceptionalHaltReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    return failed(
        gasUsedByTransaction,
        gasRemaining,
        gasUsedByTransaction,
        0,
        validationResult,
        revertReason,
        exceptionalHaltReason,
        partialBlockAccessView);
  }

  public static TransactionProcessingResult successful(
      final List<Log> logs,
      final long gasUsedByTransaction,
      final long gasRemaining,
      final long gasSpent,
      final long stateGasUsed,
      final Bytes output,
      final Optional<PartialBlockAccessView> partialBlockAccessView,
      final ValidationResult<TransactionInvalidReason> validationResult) {
    return new TransactionProcessingResult(
        Status.SUCCESSFUL,
        logs,
        gasUsedByTransaction,
        gasRemaining,
        gasSpent,
        stateGasUsed,
        output,
        validationResult,
        Optional.empty(),
        partialBlockAccessView);
  }

  /**
   * Factory method for successful transactions where gasSpent equals gasUsedByTransaction. This is
   * the standard behavior for pre-Amsterdam forks where block gas accounting uses post-refund gas.
   */
  public static TransactionProcessingResult successful(
      final List<Log> logs,
      final long gasUsedByTransaction,
      final long gasRemaining,
      final Bytes output,
      final Optional<PartialBlockAccessView> partialBlockAccessView,
      final ValidationResult<TransactionInvalidReason> validationResult) {
    return successful(
        logs,
        gasUsedByTransaction,
        gasRemaining,
        gasUsedByTransaction,
        0,
        output,
        partialBlockAccessView,
        validationResult);
  }

  /**
   * Constructor without gasSpent (for pre-Amsterdam forks). Defaults gasSpent to
   * estimateGasUsedByTransaction.
   */
  public TransactionProcessingResult(
      final Status status,
      final List<Log> logs,
      final long estimateGasUsedByTransaction,
      final long gasRemaining,
      final Bytes output,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    this(
        status,
        logs,
        estimateGasUsedByTransaction,
        gasRemaining,
        estimateGasUsedByTransaction,
        0,
        output,
        validationResult,
        revertReason,
        partialBlockAccessView);
  }

  /** Constructor with gasSpent (for Amsterdam+ forks with EIP-7778). */
  public TransactionProcessingResult(
      final Status status,
      final List<Log> logs,
      final long estimateGasUsedByTransaction,
      final long gasRemaining,
      final long gasSpent,
      final long stateGasUsed,
      final Bytes output,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    this.status = status;
    this.logs = logs;
    this.estimateGasUsedByTransaction = estimateGasUsedByTransaction;
    this.gasRemaining = gasRemaining;
    this.gasSpent = gasSpent;
    this.stateGasUsed = stateGasUsed;
    this.output = output;
    this.validationResult = validationResult;
    this.revertReason = revertReason;
    this.exceptionalHaltReason = Optional.empty();
    this.partialBlockAccessView = partialBlockAccessView;
  }

  /**
   * Constructor without gasSpent (for pre-Amsterdam forks). Defaults gasSpent to
   * estimateGasUsedByTransaction.
   */
  public TransactionProcessingResult(
      final Status status,
      final List<Log> logs,
      final long estimateGasUsedByTransaction,
      final long gasRemaining,
      final Bytes output,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<ExceptionalHaltReason> exceptionalHaltReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    this(
        status,
        logs,
        estimateGasUsedByTransaction,
        gasRemaining,
        estimateGasUsedByTransaction,
        0,
        output,
        validationResult,
        revertReason,
        exceptionalHaltReason,
        partialBlockAccessView);
  }

  /** Constructor with gasSpent and stateGasUsed (for Amsterdam+ forks with EIP-7778/EIP-8037). */
  public TransactionProcessingResult(
      final Status status,
      final List<Log> logs,
      final long estimateGasUsedByTransaction,
      final long gasRemaining,
      final long gasSpent,
      final long stateGasUsed,
      final Bytes output,
      final ValidationResult<TransactionInvalidReason> validationResult,
      final Optional<Bytes> revertReason,
      final Optional<ExceptionalHaltReason> exceptionalHaltReason,
      final Optional<PartialBlockAccessView> partialBlockAccessView) {
    this.status = status;
    this.logs = logs;
    this.estimateGasUsedByTransaction = estimateGasUsedByTransaction;
    this.gasRemaining = gasRemaining;
    this.gasSpent = gasSpent;
    this.stateGasUsed = stateGasUsed;
    this.output = output;
    this.validationResult = validationResult;
    this.revertReason = revertReason;
    this.exceptionalHaltReason = exceptionalHaltReason;
    this.partialBlockAccessView = partialBlockAccessView;
  }

  /**
   * Return the logs produced by the transaction.
   *
   * <p>This is only valid when {@code TransactionProcessor#isSuccessful} returns {@code true}.
   *
   * @return the logs produced by the transaction
   */
  @Override
  public List<Log> getLogs() {
    return logs;
  }

  /**
   * Returns the gas remaining after the transaction was processed.
   *
   * <p>This is only valid when {@code TransactionProcessor#isSuccessful} returns {@code true}.
   *
   * @return the gas remaining after the transaction was processed
   */
  @Override
  public long getGasRemaining() {
    return gasRemaining;
  }

  /**
   * Returns the estimated gas used by the transaction for block gas accounting purposes.
   *
   * <p>This value has different semantics depending on the protocol version:
   *
   * <ul>
   *   <li><b>Pre-Prague:</b> Equals the execution gas (gasLimit - gasRemaining), same as what would
   *       be charged to the user before refunds.
   *   <li><b>Prague+ (EIP-7623):</b> {@code max(executionGas, floorCost)} where floorCost is
   *       calculated based on calldata costs. This implements the "floor gas" mechanism that
   *       prevents calldata-heavy transactions from paying less than their fair share of block
   *       space.
   *   <li><b>Amsterdam+ (EIP-7778):</b> This value is used for block gas limit accounting instead
   *       of the post-refund gas, preventing block gas limit circumvention through refund credits.
   * </ul>
   *
   * <p>Note: This value is set by {@link
   * org.hyperledger.besu.ethereum.mainnet.MainnetTransactionProcessor} during transaction
   * processing.
   *
   * @return the estimated gas used for block accounting purposes
   */
  @Override
  public long getEstimateGasUsedByTransaction() {
    return estimateGasUsedByTransaction;
  }

  /**
   * Returns the gas spent by the transaction (post-refund). This is the amount the user actually
   * pays for the transaction, after SSTORE refunds have been applied.
   *
   * <p>EIP-7778: This value is now tracked separately from the gas used for block accounting
   * purposes. While block gas accounting uses pre-refund gas (estimateGasUsedByTransaction), the
   * user still receives the benefit of refunds (gasSpent = gasLimit - refundedGas).
   *
   * @return the gas spent post-refund
   */
  public long getGasSpent() {
    return gasSpent;
  }

  /**
   * Returns the state gas used by the transaction (EIP-8037).
   *
   * <p>This represents the gas consumed by state-creation operations (CREATE, SSTORE 0→nonzero,
   * CALL to new accounts, code deposits, EIP-7702 delegations). State gas is tracked separately
   * from regular gas for multidimensional gas metering.
   *
   * @return the state gas used
   */
  public long getStateGasUsed() {
    return stateGasUsed;
  }

  /**
   * Returns the status of the transaction after being processed.
   *
   * @return the status of the transaction after being processed
   */
  public Status getStatus() {
    return status;
  }

  @Override
  public Bytes getOutput() {
    return output;
  }

  /**
   * Returns whether the transaction was invalid.
   *
   * @return {@code true} if the transaction was invalid; otherwise {@code false}
   */
  @Override
  public boolean isInvalid() {
    return getStatus() == Status.INVALID;
  }

  /**
   * Returns whether the transaction was successfully processed.
   *
   * @return {@code true} if the transaction was successfully processed; otherwise {@code false}
   */
  @Override
  public boolean isSuccessful() {
    return getStatus() == Status.SUCCESSFUL;
  }

  /**
   * Returns whether the transaction failed.
   *
   * @return {@code true} if the transaction failed; otherwise {@code false}
   */
  @Override
  public boolean isFailed() {
    return getStatus() == Status.FAILED;
  }

  /**
   * Returns the transaction validation result.
   *
   * @return the validation result, with the reason for failure (if applicable.)
   */
  public ValidationResult<TransactionInvalidReason> getValidationResult() {
    return validationResult;
  }

  /**
   * Returns the transaction block access view
   *
   * @return block access view of the executed transaction , empty when failed
   */
  public Optional<PartialBlockAccessView> getPartialBlockAccessView() {
    return partialBlockAccessView;
  }

  /**
   * Set isProcessedInParallel to the value in parameter
   *
   * @param isProcessedInParallel new value of isProcessedInParallel
   */
  public void setIsProcessedInParallel(final Optional<Boolean> isProcessedInParallel) {
    this.isProcessedInParallel = isProcessedInParallel;
  }

  /**
   * Returns a flag that indicates if the transaction was executed in parallel
   *
   * @return Optional of Boolean, the value of the boolean is true if the transaction was executed
   *     in parallel
   */
  public Optional<Boolean> getIsProcessedInParallel() {
    return isProcessedInParallel;
  }

  /**
   * Returns the reason why a transaction was reverted (if applicable).
   *
   * @return the revert reason.
   */
  @Override
  public Optional<Bytes> getRevertReason() {
    return revertReason;
  }

  @Override
  public Optional<String> getInvalidReason() {
    return (validationResult.isValid()
        ? Optional.empty()
        : Optional.of(validationResult.getErrorMessage()));
  }

  public Optional<ExceptionalHaltReason> getExceptionalHaltReason() {
    return exceptionalHaltReason;
  }

  @Override
  public String toString() {
    return "TransactionProcessingResult{"
        + "status="
        + status
        + ", estimateGasUsedByTransaction="
        + estimateGasUsedByTransaction
        + ", gasRemaining="
        + gasRemaining
        + ", gasSpent="
        + gasSpent
        + ", stateGasUsed="
        + stateGasUsed
        + ", logs="
        + logs
        + ", output="
        + output
        + ", validationResult="
        + validationResult
        + ", revertReason="
        + revertReason
        + '}';
  }
}
