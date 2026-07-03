/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.ethereum.transaction.exceptions;

import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.GAS_PRICE_BELOW_BASE_FEE;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.GAS_PRICE_TOO_LOW;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.INTRINSIC_GAS_EXCEEDS_GAS_LIMIT;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.UNKNOWN;
import static org.hyperledger.besu.ethereum.transaction.exceptions.BlockStateCallError.UPFRONT_COST_EXCEEDS_BALANCE;

import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

/** Exception thrown when a block state call validation fails. */
public class BlockStateCallException extends RuntimeException {

  /** The block state call error. */
  private final BlockStateCallError error;

  /**
   * Constructs an exception with a message and an error descriptor.
   *
   * @param message the message
   * @param error the block state call error
   */
  public BlockStateCallException(final String message, final BlockStateCallError error) {
    super(message);
    this.error = error;
  }

  /**
   * Returns the block state call error, if present.
   *
   * @return optional error descriptor
   */
  public BlockStateCallError getError() {
    return error;
  }

  /**
   * Constructs an exception with a message and an error descriptor.
   *
   * @param message the message
   * @param invalidReason the invalidReason
   */
  public BlockStateCallException(
      final String message, final TransactionInvalidReason invalidReason) {
    super(message);
    this.error = fromTransactionInvalidReason(invalidReason);
  }

  /**
   * Returns the error message.
   *
   * @param transactionInvalidReason The transaction invalid reason.
   * @return The error message.
   */
  private static BlockStateCallError fromTransactionInvalidReason(
      final TransactionInvalidReason transactionInvalidReason) {
    return switch (transactionInvalidReason) {
      case UPFRONT_COST_EXCEEDS_BALANCE -> UPFRONT_COST_EXCEEDS_BALANCE;
      case GAS_PRICE_TOO_LOW -> GAS_PRICE_TOO_LOW;
      case GAS_PRICE_BELOW_CURRENT_BASE_FEE -> GAS_PRICE_BELOW_BASE_FEE;
      case INTRINSIC_GAS_EXCEEDS_GAS_LIMIT -> INTRINSIC_GAS_EXCEEDS_GAS_LIMIT;
      default -> UNKNOWN;
    };
  }
}
