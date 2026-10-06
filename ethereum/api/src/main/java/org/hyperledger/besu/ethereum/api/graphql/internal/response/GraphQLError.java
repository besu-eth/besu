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
package org.hyperledger.besu.ethereum.api.graphql.internal.response;

import org.hyperledger.besu.ethereum.transaction.TransactionInvalidReason;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonGetter;

/**
 * Enum representing various types of GraphQL errors.
 *
 * <p>Each error is associated with a specific code and message. The code is an integer that
 * uniquely identifies the error. The message is a string that provides a human-readable description
 * of the error.
 *
 * <p>This enum includes standard errors such as INVALID_PARAMS and INTERNAL_ERROR, transaction
 * validation failures such as NONCE_TOO_LOW and INVALID_TRANSACTION_SIGNATURE, and private
 * transaction errors such as PRIVATE_TRANSACTION_FAILED and PRIVATE_NONCE_TOO_LOW.
 *
 * <p>The {@code of} method is used to map a {@code TransactionInvalidReason} to a corresponding
 * {@code GraphQLError}.
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum GraphQLError {
  // Standard errors
  /** Error code -32602. This error occurs when the parameters provided are invalid. */
  INVALID_PARAMS(-32602, "Invalid params"),

  /** Error code -32603. This error occurs when there is an internal error. */
  INTERNAL_ERROR(-32603, "Internal error"),

  // Transaction validation failures
  /** Error code -32001. This error occurs when the nonce value is too low. */
  NONCE_TOO_LOW(-32001, "Nonce too low"),

  /** Error code -32002. This error occurs when the transaction signature is invalid. */
  INVALID_TRANSACTION_SIGNATURE(-32002, "Invalid signature"),

  /** Error code -32003. This error occurs when the intrinsic gas exceeds the gas limit. */
  INTRINSIC_GAS_EXCEEDS_LIMIT(-32003, "Intrinsic gas exceeds gas limit"),

  /**
   * Error code -32004. This error occurs when the upfront cost of the transaction exceeds the
   * account balance.
   */
  TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE(-32004, "Upfront cost exceeds account balance"),

  /**
   * Error code -32005. This error occurs when the transaction gas limit exceeds the block gas
   * limit.
   */
  EXCEEDS_BLOCK_GAS_LIMIT(-32005, "Transaction gas limit exceeds block gas limit"),

  /** Error code -32006. This error occurs when the nonce value is too high. */
  INCORRECT_NONCE(-32006, "Nonce too high"),

  /**
   * Error code -32007. This error occurs when the sender account is not authorized to send
   * transactions.
   */
  TX_SENDER_NOT_AUTHORIZED(-32007, "Sender account not authorized to send transactions"),

  /** Error code -32008. This error occurs when the initial sync is still in progress. */
  CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE(-32008, "Initial sync is still in progress"),

  /**
   * Error code -32009. This error occurs when the gas price is below the configured minimum gas
   * price.
   */
  GAS_PRICE_TOO_LOW(-32009, "Gas price below configured minimum gas price"),

  /**
   * Error code -32000. This error occurs when the Chain ID in the transaction signature is wrong.
   */
  WRONG_CHAIN_ID(-32000, "Wrong Chain ID in transaction signature"),

  /**
   * Error code -32000. This error occurs when signatures with replay protection are not supported.
   */
  REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED(
      -32000, "Signatures with replay protection are not supported"),

  /** Error code -32000. This error occurs when the transaction fee cap is exceeded. */
  TX_FEECAP_EXCEEDED(-32000, "Transaction fee cap exceeded");

  private final int code;
  private final String message;

  GraphQLError(final int code, final String message) {
    this.code = code;
    this.message = message;
  }

  /**
   * Returns the error code associated with this GraphQL error.
   *
   * @return the error code as an integer.
   */
  @JsonGetter("code")
  public int getCode() {
    return code;
  }

  /**
   * Returns the error message associated with this GraphQL error.
   *
   * @return the error message as a string.
   */
  @JsonGetter("message")
  public String getMessage() {
    return message;
  }

  /**
   * Maps a {@code TransactionInvalidReason} to a corresponding {@code GraphQLError}.
   *
   * <p>This method is used to convert a transaction invalid reason to a GraphQL error, which can be
   * used to provide more detailed error information to the client.
   *
   * @param transactionInvalidReason the transaction invalid reason to be converted.
   * @return the corresponding GraphQL error.
   */
  public static GraphQLError of(final TransactionInvalidReason transactionInvalidReason) {
    return switch (transactionInvalidReason) {
      case WRONG_CHAIN_ID -> WRONG_CHAIN_ID;
      case REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED -> REPLAY_PROTECTED_SIGNATURES_NOT_SUPPORTED;
      case INVALID_SIGNATURE -> INVALID_TRANSACTION_SIGNATURE;
      case UPFRONT_GAS_COST_EXCEEDS_BALANCE -> TRANSACTION_UPFRONT_COST_EXCEEDS_BALANCE;
      case NONCE_TOO_LOW -> NONCE_TOO_LOW;
      case NONCE_TOO_HIGH -> INCORRECT_NONCE;
      case INTRINSIC_GAS_EXCEEDS_GAS_LIMIT -> INTRINSIC_GAS_EXCEEDS_LIMIT;
      case EXCEEDS_BLOCK_GAS_LIMIT -> EXCEEDS_BLOCK_GAS_LIMIT;
      case TX_SENDER_NOT_AUTHORIZED -> TX_SENDER_NOT_AUTHORIZED;
      case CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE -> CHAIN_HEAD_WORLD_STATE_NOT_AVAILABLE;
      case GAS_PRICE_TOO_LOW -> GAS_PRICE_TOO_LOW;
      case TX_FEECAP_EXCEEDED -> TX_FEECAP_EXCEEDED;
      default -> INTERNAL_ERROR;
    };
  }
}
