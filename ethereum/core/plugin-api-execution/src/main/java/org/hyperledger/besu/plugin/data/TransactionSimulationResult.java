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

import org.hyperledger.besu.datatypes.Transaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The result of simulating a transaction: the transaction that was run and the outcome of
 * processing it.
 *
 * @param transaction the simulated transaction
 * @param result the processing result of the simulation
 */
public record TransactionSimulationResult(
    Transaction transaction, TransactionProcessingResult result) {

  /**
   * Was the simulation successful?
   *
   * @return true if the transaction was processed successfully
   */
  public boolean isSuccessful() {
    return result.isSuccessful();
  }

  /**
   * Was the transaction invalid?
   *
   * @return true if the transaction failed validation and was not executed
   */
  public boolean isInvalid() {
    return result.isInvalid();
  }

  /**
   * Return the optional revert reason
   *
   * @return the optional revert reason
   */
  public Optional<Bytes> getRevertReason() {
    return result.getRevertReason();
  }

  /**
   * Return the optional invalid reason
   *
   * @return the optional invalid reason
   */
  public Optional<String> getInvalidReason() {
    return result.getInvalidReason();
  }

  /**
   * Estimated gas used by the transaction
   *
   * @return estimated gas used
   */
  public long getGasEstimate() {
    return transaction.getGasLimit() - result.getGasRemaining();
  }
}
