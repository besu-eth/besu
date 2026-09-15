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
package org.hyperledger.besu.plugin.services;

import org.hyperledger.besu.datatypes.PendingTransaction;
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;
import org.hyperledger.besu.plugin.services.txselection.BlockTransactionSelectionService;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelector;
import org.hyperledger.besu.plugin.services.txselection.PluginTransactionSelectorFactory;
import org.hyperledger.besu.plugin.services.txselection.SelectorsStateManager;

import java.util.List;

/** Transaction selection service interface */
@Unstable
public interface TransactionSelectionService extends BesuService {

  /**
   * Create a transaction selector plugin
   *
   * @param pendingBlockHeader the header of the block being created
   * @param selectorsStateManager the selectors state manager
   * @return the transaction selector plugin
   */
  PluginTransactionSelector createPluginTransactionSelector(
      ProcessableBlockHeader pendingBlockHeader, SelectorsStateManager selectorsStateManager);

  /**
   * Called during the block creation to allow plugins to propose their own pending transactions for
   * block inclusion
   *
   * @param blockTransactionSelectionService the service used by the plugin to evaluate pending
   *     transactions and commit or rollback changes
   * @param pendingBlockHeader the header of the block being created
   * @param candidatePendingTransactions the unmodifiable list of candidate pending transactions
   *     from the public pool, in the order they will be evaluated
   */
  void selectPendingTransactions(
      BlockTransactionSelectionService blockTransactionSelectionService,
      final ProcessableBlockHeader pendingBlockHeader,
      final List<? extends PendingTransaction> candidatePendingTransactions);

  /**
   * Registers the transaction selector factory with the service
   *
   * @param transactionSelectorFactory transaction selector factory to be used
   */
  void registerPluginTransactionSelectorFactory(
      PluginTransactionSelectorFactory transactionSelectorFactory);
}
