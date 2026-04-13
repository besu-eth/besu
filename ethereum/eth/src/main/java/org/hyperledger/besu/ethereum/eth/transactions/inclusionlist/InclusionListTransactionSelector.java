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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;

import java.util.List;

/**
 * Interface for selecting transactions to include in an EIP-7805 inclusion list. Implementations
 * define the strategy for choosing which mempool transactions should be included.
 */
public interface InclusionListTransactionSelector {

  /**
   * Selects transactions from the mempool for inclusion in an inclusion list.
   *
   * @param parentHash the hash of the parent block
   * @param pendingTransactionsBySender the candidate transactions from the transaction pool grouped
   *     by sender
   * @param maxBytes the maximum total bytes allowed for selected transactions
   * @return the selected pending transactions
   */
  List<PendingTransaction> selectTransactions(
      Hash parentHash, List<List<PendingTransaction>> pendingTransactionsBySender, int maxBytes);
}
