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
package org.hyperledger.besu.plugin.services.txselection;

import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.List;

/**
 * A service that can be used by plugins to include their pending transactions during block
 * creation. Proposed pending transactions need to be first evaluated and based on the result of the
 * evaluation of one or more pending transactions the plugin can apply its logic to commit or not
 * the inclusion of the evaluated in the block.
 *
 * <p>The process of including plugin proposed pending transactions starts when {@link
 * PluginTransactionSelectorFactory#selectPendingTransactions(BlockTransactionSelectionService,
 * ProcessableBlockHeader, List)} is called.
 */
public interface BlockTransactionSelectionService {

  /**
   * Evaluate a plugin proposed pending transaction for block inclusion
   *
   * @param pendingTransaction the pending transaction proposed by the plugin
   * @return the result of the evaluation
   */
  TransactionSelectionResult evaluatePendingTransaction(PendingTransaction pendingTransaction);

  /**
   * Commit the changes applied by all the evaluated pending transactions since the previous commit.
   * The state of every {@link PluginTransactionSelector} is committed as part of this call.
   *
   * @return false only if a timeout occurred during the selection of the pending transaction,
   *     meaning that the pending transaction is not included in the current block
   */
  boolean commit();

  /**
   * Rollback the changes applied by all the evaluated pending transactions since the previous
   * commit.
   *
   * <p>The state of every {@link PluginTransactionSelector} is rolled back as part of this call.
   */
  void rollback();
}
