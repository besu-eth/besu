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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import java.util.Objects;
import java.util.Optional;

public final class TransactionAddedResult {
  private enum Status {
    INVALID,
    REPLACED,
    DROPPED,
    TRY_NEXT_LAYER,
    ADDED,
    INTERNAL_ERROR
  }

  public static final TransactionAddedResult ALREADY_KNOWN =
      new TransactionAddedResult(TransactionInvalidReason.TRANSACTION_ALREADY_KNOWN);
  public static final TransactionAddedResult REJECTED_UNDERPRICED_REPLACEMENT =
      new TransactionAddedResult(TransactionInvalidReason.TRANSACTION_REPLACEMENT_UNDERPRICED);
  public static final TransactionAddedResult NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER =
      new TransactionAddedResult(TransactionInvalidReason.NONCE_TOO_FAR_IN_FUTURE_FOR_SENDER);

  public static final TransactionAddedResult ADDED = new TransactionAddedResult(Status.ADDED);
  public static final TransactionAddedResult TRY_NEXT_LAYER =
      new TransactionAddedResult(Status.TRY_NEXT_LAYER);

  public static final TransactionAddedResult DROPPED = new TransactionAddedResult(Status.DROPPED);

  public static final TransactionAddedResult INTERNAL_ERROR =
      new TransactionAddedResult(Status.INTERNAL_ERROR);
  public static final TransactionAddedResult DISABLED =
      new TransactionAddedResult(TransactionInvalidReason.TX_POOL_DISABLED);

  private final Optional<TransactionInvalidReason> rejectReason;

  private final Optional<PendingTransaction> replacedTransaction;

  private final Status status;

  private TransactionAddedResult(final PendingTransaction replacedTransaction) {
    this.replacedTransaction = Optional.of(replacedTransaction);
    this.rejectReason = Optional.empty();
    this.status = Status.REPLACED;
  }

  private TransactionAddedResult(final TransactionInvalidReason rejectReason) {
    this.replacedTransaction = Optional.empty();
    this.rejectReason = Optional.of(rejectReason);
    this.status = Status.INVALID;
  }

  private TransactionAddedResult(final Status status) {
    this.replacedTransaction = Optional.empty();
    this.rejectReason = Optional.empty();
    this.status = status;
  }

  public boolean isSuccess() {
    return !isRejected() && status != Status.INTERNAL_ERROR;
  }

  public boolean isRejected() {
    return status == Status.INVALID;
  }

  public boolean isReplacement() {
    return replacedTransaction.isPresent();
  }

  public Optional<TransactionInvalidReason> maybeInvalidReason() {
    return rejectReason;
  }

  public Optional<PendingTransaction> maybeReplacedTransaction() {
    return replacedTransaction;
  }

  /**
   * A rejection for the given reason.
   *
   * @param reason why the transaction was rejected
   * @return the rejected result
   */
  public static TransactionAddedResult rejected(final TransactionInvalidReason reason) {
    return new TransactionAddedResult(reason);
  }

  public static TransactionAddedResult createForReplacement(
      final PendingTransaction replacedTransaction) {
    return new TransactionAddedResult(replacedTransaction);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransactionAddedResult that = (TransactionAddedResult) o;
    return Objects.equals(rejectReason, that.rejectReason)
        && Objects.equals(replacedTransaction, that.replacedTransaction)
        && status == that.status;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rejectReason, replacedTransaction, status);
  }

  @Override
  public String toString() {
    return "TransactionAddedResult{"
        + "rejectReason="
        + rejectReason
        + ", replacedTransaction="
        + replacedTransaction
        + ", status="
        + status
        + '}';
  }
}
