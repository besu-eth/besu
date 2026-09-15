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
import org.hyperledger.besu.plugin.Unstable;
import org.hyperledger.besu.plugin.data.ProcessableBlockHeader;

import java.util.List;

/**
 * Interface for a factory that creates transaction selector and propose pending transaction for
 * block inclusion
 */
@Unstable
public interface PluginTransactionSelectorFactory {
  /**
   * Create a plugin transaction selector, that can be used during block creation to apply custom
   * filters to proposed pending transactions
   *
   * @param selectorsStateManager the selectors state manager
   * @return the transaction selector
   * @deprecated use {@link PluginTransactionSelectorFactory#create(ProcessableBlockHeader,
   *     SelectorsStateManager)} instead
   */
  @Deprecated(forRemoval = true)
  default PluginTransactionSelector create(final SelectorsStateManager selectorsStateManager) {
    return PluginTransactionSelector.ACCEPT_ALL;
  }

  /**
   * Create a plugin transaction selector, that can be used during block creation to apply custom
   * filters to proposed pending transactions
   *
   * @param pendingBlockHeader the header of the block being created
   * @param selectorsStateManager the selectors state manager
   * @return the transaction selector
   */
  default PluginTransactionSelector create(
      final ProcessableBlockHeader pendingBlockHeader,
      final SelectorsStateManager selectorsStateManager) {
    return create(selectorsStateManager);
  }

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
  default void selectPendingTransactions(
      final BlockTransactionSelectionService blockTransactionSelectionService,
      final ProcessableBlockHeader pendingBlockHeader,
      final List<? extends PendingTransaction> candidatePendingTransactions) {}
}
