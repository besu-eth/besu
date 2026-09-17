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

import org.hyperledger.besu.datatypes.Log;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * This interface represents the result of processing a transaction. It provides methods to access
 * various details about the transaction processing result such as logs, gas remaining, output, and
 * status.
 */
public interface TransactionProcessingResult {

  /**
   * Return the logs produced by the transaction.
   *
   * <p>This is only valid when {@code TransactionProcessor#isSuccessful} returns {@code true}.
   *
   * @return the logs produced by the transaction
   */
  List<Log> getLogs();

  /**
   * Returns the gas remaining after the transaction was processed.
   *
   * <p>This is only valid when {@code TransactionProcessor#isSuccessful} returns {@code true}.
   *
   * @return the gas remaining after the transaction was processed
   */
  long getGasRemaining();

  /**
   * Returns the estimate gas used by the transaction, the difference between the transactions gas
   * limit and the remaining gas
   *
   * @return the estimate gas used
   */
  long getEstimateGasUsedByTransaction();

  /**
   * Returns the output.
   *
   * @return the output.
   */
  Bytes getOutput();

  /**
   * Returns whether the transaction was invalid.
   *
   * @return {@code true} if the transaction was invalid; otherwise {@code false}
   */
  boolean isInvalid();

  /**
   * Returns whether the transaction was successfully processed.
   *
   * @return {@code true} if the transaction was successfully processed; otherwise {@code false}
   */
  boolean isSuccessful();

  /**
   * Returns whether the transaction failed.
   *
   * @return {@code true} if the transaction failed; otherwise {@code false}
   */
  boolean isFailed();

  /**
   * Returns the reason why a transaction was reverted (if applicable).
   *
   * @return the revert reason.
   */
  Optional<Bytes> getRevertReason();

  /**
   * Return the reason why the transaction is invalid or empty if the transaction is successful
   *
   * @return the optional invalid reason as a string
   */
  Optional<String> getInvalidReason();
}
