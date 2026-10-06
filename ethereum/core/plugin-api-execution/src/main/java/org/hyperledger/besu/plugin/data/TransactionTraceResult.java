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
package org.hyperledger.besu.plugin.data;

import org.hyperledger.besu.datatypes.Hash;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents the result of tracing a transaction, including its status and optional error message.
 */
public class TransactionTraceResult {
  /** Enumeration representing the status of the transaction trace. */
  public enum Status {
    /**
     * The transaction was traced successfully. This might include transactions that have been
     * reverted.
     */
    SUCCESS,
    /** There was an internal error while generating the trace. */
    ERROR
  }

  private final Hash txHash;
  private final Status status;
  private final String errorMessage;

  private TransactionTraceResult(
      final Hash txHash, final Status status, final String errorMessage) {
    this.txHash = txHash;
    this.status = status;
    this.errorMessage = errorMessage;
  }

  /**
   * Creates a TransactionTraceResult with a successful status and the given transaction hash.
   *
   * @param txHash The hash of the traced transaction.
   * @return A successful TransactionTraceResult.
   */
  public static TransactionTraceResult success(final Hash txHash) {
    return new TransactionTraceResult(txHash, Status.SUCCESS, null);
  }

  /**
   * Creates a TransactionTraceResult with an error status, the given transaction hash, and an error
   * message.
   *
   * @param txHash The hash of the traced transaction.
   * @param errorMessage An error message describing the issue encountered during tracing.
   * @return An error TransactionTraceResult.
   */
  public static TransactionTraceResult error(final Hash txHash, final String errorMessage) {
    return new TransactionTraceResult(txHash, Status.ERROR, errorMessage);
  }

  /**
   * Get the hash of the traced transaction.
   *
   * @return The hash of the transaction.
   */
  public Hash getTxHash() {
    return txHash;
  }

  /**
   * Get the status of the transaction trace.
   *
   * @return The status of the transaction trace.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Get an optional error message associated with the transaction trace.
   *
   * @return An optional error message, which may be empty if no error occurred.
   */
  public Optional<String> errorMessage() {
    return Optional.ofNullable(errorMessage);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TransactionTraceResult that = (TransactionTraceResult) o;
    return Objects.equals(txHash, that.txHash)
        && status == that.status
        && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(txHash, status, errorMessage);
  }

  @Override
  public String toString() {
    return "TransactionTraceResult{"
        + "txHash="
        + txHash
        + ", status="
        + status
        + ", errorMessage='"
        + errorMessage
        + '\''
        + '}';
  }
}
