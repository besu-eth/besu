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

import java.util.Objects;
import java.util.Optional;

/**
 * Represent the result of the selection process of a candidate transaction, during the block
 * creation phase.
 */
public class TransactionSelectionResult {

  /**
   * Represent the status of a transaction selection result. Plugin can extend this to implement its
   * own statuses.
   */
  protected interface Status {
    /**
     * Should the selection process be stopped?
     *
     * @return true if the selection process needs to be stopped
     */
    boolean stop();

    /**
     * Should the current transaction be removed from the txpool?
     *
     * @return yes if the transaction should be removed from the txpool
     */
    boolean discard();

    /**
     * Should the score of this transaction be decremented?
     *
     * @return yes if the score of this transaction needs to be decremented
     */
    boolean penalize();

    /**
     * Name of this status
     *
     * @return the name
     */
    String name();
  }

  private enum BaseStatus implements Status {
    SELECTED,
    SELECTED_ROLLBACK(false, false, false),
    BLOCK_FULL(true, false, false),
    BLOBS_FULL(false, false, false),
    BLOCK_OCCUPANCY_ABOVE_THRESHOLD(true, false, false),
    BLOCK_SIZE_ABOVE_THRESHOLD(true, false, false),
    SELECTION_CANCELLED(true, false, false),
    BLOCK_SELECTION_TIMEOUT(true, false, false),
    BLOCK_SELECTION_TIMEOUT_INVALID_TX(true, true, true),
    PLUGIN_SELECTION_TIMEOUT(false, false, false),
    PLUGIN_SELECTION_TIMEOUT_INVALID_TX(false, true, true),
    TX_EVALUATION_TOO_LONG(true, false, true),
    INVALID_TX_EVALUATION_TOO_LONG(true, true, true),
    INVALID_TRANSIENT(false, false, false),
    INVALID_PENALIZED(false, false, true),
    INVALID(false, true, false),
    INTERNAL_ERROR(false, true, true);

    private final boolean stop;
    private final boolean discard;
    private final boolean penalize;

    BaseStatus() {
      this.stop = false;
      this.discard = false;
      this.penalize = false;
    }

    BaseStatus(final boolean stop, final boolean discard, final boolean penalize) {
      this.stop = stop;
      this.discard = discard;
      this.penalize = penalize;
    }

    @Override
    public String toString() {
      return name() + " (stop=" + stop + ", discard=" + discard + ", penalize=" + penalize + ")";
    }

    @Override
    public boolean stop() {
      return stop;
    }

    @Override
    public boolean discard() {
      return discard;
    }

    @Override
    public boolean penalize() {
      return penalize;
    }
  }

  /** The transaction has been selected to be included in the new block */
  public static final TransactionSelectionResult SELECTED =
      new TransactionSelectionResult(BaseStatus.SELECTED);

  /**
   * Relate to transactions that are evaluated in group (aka bundle), when a previously selected
   * transaction decision is reverted due to the selection result of a following transaction in the
   * same group
   */
  public static final TransactionSelectionResult SELECTED_ROLLBACK =
      new TransactionSelectionResult(BaseStatus.SELECTED_ROLLBACK);

  /** The transaction has not been selected since the block is full. */
  public static final TransactionSelectionResult BLOCK_FULL =
      new TransactionSelectionResult(BaseStatus.BLOCK_FULL);

  /** The block already contains the max number of allowed blobs. */
  public static final TransactionSelectionResult BLOBS_FULL =
      new TransactionSelectionResult(BaseStatus.BLOBS_FULL);

  /** The block creation has been cancelled */
  public static final TransactionSelectionResult SELECTION_CANCELLED =
      new TransactionSelectionResult(BaseStatus.SELECTION_CANCELLED);

  /** There was no more time to add transaction to the block */
  public static final TransactionSelectionResult BLOCK_SELECTION_TIMEOUT =
      new TransactionSelectionResult(BaseStatus.BLOCK_SELECTION_TIMEOUT);

  /** There was no more time to add transaction to the block, and the transaction is invalid */
  public static final TransactionSelectionResult BLOCK_SELECTION_TIMEOUT_INVALID_TX =
      new TransactionSelectionResult(BaseStatus.BLOCK_SELECTION_TIMEOUT_INVALID_TX);

  /** There was no more time for plugins to add transaction to the block */
  public static final TransactionSelectionResult PLUGIN_SELECTION_TIMEOUT =
      new TransactionSelectionResult(BaseStatus.PLUGIN_SELECTION_TIMEOUT);

  /**
   * There was no more time for plugins to add transaction to the block, and the transaction is
   * invalid
   */
  public static final TransactionSelectionResult PLUGIN_SELECTION_TIMEOUT_INVALID_TX =
      new TransactionSelectionResult(BaseStatus.PLUGIN_SELECTION_TIMEOUT_INVALID_TX);

  /** Transaction took too much to evaluate, but it was valid */
  public static final TransactionSelectionResult TX_EVALUATION_TOO_LONG =
      new TransactionSelectionResult(BaseStatus.TX_EVALUATION_TOO_LONG);

  /** Transaction took too much to evaluate, and it was invalid */
  public static final TransactionSelectionResult INVALID_TX_EVALUATION_TOO_LONG =
      new TransactionSelectionResult(BaseStatus.INVALID_TX_EVALUATION_TOO_LONG);

  /**
   * There was an unhandled exception during the evaluation of the transaction. If this occurs, it
   * indicates there is a bug somewhere.
   */
  public static final TransactionSelectionResult INTERNAL_ERROR =
      new TransactionSelectionResult(BaseStatus.INTERNAL_ERROR);

  /**
   * The transaction has not been selected since its gas limit is greater than the block remaining
   * gas, but the selection should continue.
   */
  public static final TransactionSelectionResult TX_TOO_LARGE_FOR_REMAINING_GAS =
      TransactionSelectionResult.invalidTransient("TX_TOO_LARGE_FOR_REMAINING_GAS");

  /**
   * The transaction has not been selected since there is not enough remaining blob gas in the block
   * to fit the blobs of the tx, but selection should continue.
   */
  public static final TransactionSelectionResult TX_TOO_LARGE_FOR_REMAINING_BLOB_GAS =
      TransactionSelectionResult.invalidTransient("TX_TOO_LARGE_FOR_REMAINING_BLOB_GAS");

  /**
   * The transaction has not been selected since there is not enough remaining block bytes in the
   * block to fit the transaction, but selection should continue.
   */
  public static final TransactionSelectionResult TOO_LARGE_FOR_REMAINING_BLOCK_SIZE =
      TransactionSelectionResult.invalidTransient("TOO_LARGE_FOR_REMAINING_BLOCK_SIZE");

  /**
   * The transaction has not been selected because merging its execution access view would exceed
   * the EIP-7928 block access list item budget for the pending block gas limit, but selection
   * should continue.
   */
  public static final TransactionSelectionResult BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED =
      TransactionSelectionResult.invalidTransient("BLOCK_ACCESS_LIST_ITEM_BUDGET_EXCEEDED");

  /**
   * The transaction has not been selected since its current price is below the configured min
   * price, but the selection should continue.
   */
  public static final TransactionSelectionResult CURRENT_TX_PRICE_BELOW_MIN =
      TransactionSelectionResult.invalidPenalized("CURRENT_TX_PRICE_BELOW_MIN");

  /**
   * The transaction has not been selected since its blob price is below the current network blob
   * price, but the selection should continue.
   */
  public static final TransactionSelectionResult BLOB_PRICE_BELOW_CURRENT_MIN =
      TransactionSelectionResult.invalidPenalized("BLOB_PRICE_BELOW_CURRENT_MIN");

  /**
   * The transaction has not been selected since its priority fee is below the configured min
   * priority fee per gas, but the selection should continue.
   */
  public static final TransactionSelectionResult PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN =
      TransactionSelectionResult.invalidPenalized("PRIORITY_FEE_PER_GAS_BELOW_CURRENT_MIN");

  /**
   * The transaction has not been selected since its sender already had a previous transaction not
   * selected
   */
  public static final TransactionSelectionResult SENDER_WITH_PREVIOUS_TX_NOT_SELECTED =
      TransactionSelectionResult.invalidTransient("SENDER_WITH_PREVIOUS_TX_NOT_SELECTED");

  private final Status status;
  private final Optional<String> maybeInvalidReason;

  /**
   * Create a new transaction selection result with the passed status
   *
   * @param status the selection result status
   */
  protected TransactionSelectionResult(final Status status) {
    this(status, null);
  }

  /**
   * Create a new transaction selection result with the passed status and invalid reason
   *
   * @param status the selection result status
   * @param invalidReason string with a custom invalid reason
   */
  protected TransactionSelectionResult(final Status status, final String invalidReason) {
    this.status = status;
    this.maybeInvalidReason = Optional.ofNullable(invalidReason);
  }

  /**
   * Return a selection result that identify the candidate transaction as temporarily invalid, this
   * means that the transaction could become valid at a later time.
   *
   * @param invalidReason the reason why transaction is invalid
   * @return the selection result
   */
  public static TransactionSelectionResult invalidTransient(final String invalidReason) {
    return new TransactionSelectionResult(BaseStatus.INVALID_TRANSIENT, invalidReason);
  }

  /**
   * Return a selection result that identify the candidate transaction as temporarily invalid and
   * that it should be penalized, this means that the transaction could become valid at a later
   * time.
   *
   * @param invalidReason the reason why transaction is invalid
   * @return the selection result
   */
  public static TransactionSelectionResult invalidPenalized(final String invalidReason) {
    return new TransactionSelectionResult(BaseStatus.INVALID_PENALIZED, invalidReason);
  }

  /**
   * Return a selection result that identify the candidate transaction as permanently invalid, this
   * means that it could be removed safely from the transaction pool.
   *
   * @param invalidReason the reason why transaction is invalid
   * @return the selection result
   */
  public static TransactionSelectionResult invalid(final String invalidReason) {
    return new TransactionSelectionResult(BaseStatus.INVALID, invalidReason);
  }

  /**
   * Is the block creation done and the selection process should stop?
   *
   * @return true if the selection process should stop, false otherwise
   */
  public boolean stop() {
    return status.stop();
  }

  /**
   * Should the candidate transaction removed from the transaction pool?
   *
   * @return true if the candidate transaction should be removed from transaction pool, false
   *     otherwise
   */
  public boolean discard() {
    return status.discard();
  }

  /**
   * Should the score of this transaction be decremented?
   *
   * @return yes if the score of this transaction needs to be decremented
   */
  public boolean penalize() {
    return status.penalize();
  }

  /**
   * Is the candidate transaction selected for block inclusion?
   *
   * @return true if the candidate transaction is included in the new block, false otherwise
   */
  public boolean selected() {
    return BaseStatus.SELECTED.equals(status);
  }

  /**
   * Optionally return the reason why the transaction is invalid if present
   *
   * @return an optional with the invalid reason
   */
  public Optional<String> maybeInvalidReason() {
    return maybeInvalidReason;
  }

  @Override
  public String toString() {
    return status.name() + maybeInvalidReason.map(ir -> "(" + ir + ")").orElse("");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final TransactionSelectionResult that = (TransactionSelectionResult) o;
    return status == that.status && Objects.equals(maybeInvalidReason, that.maybeInvalidReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(status, maybeInvalidReason);
  }
}
