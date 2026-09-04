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
package org.hyperledger.besu.plugin.services.txselection;

import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;

import com.google.common.base.Stopwatch;

/**
 * The context in which a candidate transaction is evaluated for block inclusion: the pending block
 * header, the pending transaction, the evaluation timer, the transaction gas price, the minimum gas
 * price for inclusion, and whether the evaluation has been cancelled.
 */
public interface TransactionEvaluationContext {

  /**
   * Gets the pending block header
   *
   * @return the pending block header
   */
  ProcessableBlockHeader getPendingBlockHeader();

  /**
   * Gets the pending transaction.
   *
   * @return the pending transaction
   */
  PendingTransaction getPendingTransaction();

  /**
   * Gets the stopwatch used for timing the evaluation.
   *
   * @return the evaluation timer
   */
  Stopwatch getEvaluationTimer();

  /**
   * Gets the gas price of the transaction.
   *
   * @return the transaction gas price
   */
  Wei getTransactionGasPrice();

  /**
   * Gets the min gas price for block inclusion
   *
   * @return the min gas price
   */
  Wei getMinGasPrice();

  /**
   * Returns true if the evaluation of the tx is cancelled
   *
   * @return true if the evaluation of the tx is cancelled
   */
  boolean isCancelled();
}
