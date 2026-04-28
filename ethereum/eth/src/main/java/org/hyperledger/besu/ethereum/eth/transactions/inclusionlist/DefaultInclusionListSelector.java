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
package org.hyperledger.besu.ethereum.eth.transactions.inclusionlist;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link InclusionListTransactionSelector} that selects transactions for
 * inclusion lists per EIP-7805. Transactions are prioritized by effective gas price (highest
 * first), time in pool (older first for ties), and nonce sequentiality per sender is enforced.
 */
public class DefaultInclusionListSelector implements InclusionListTransactionSelector {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultInclusionListSelector.class);

  @Override
  public List<PendingTransaction> selectTransactions(
      final Hash parentHash,
      final List<List<PendingTransaction>> pendingTransactionsBySender,
      final int maxBytes) {

    final List<PendingTransaction> selected = new ArrayList<>();

    int totalBytes = 0;
    boolean maxSizeReached = false;

    goToNextSender:
    for (List<PendingTransaction> senderPendingTransactions : pendingTransactionsBySender) {
      for (PendingTransaction pendingTransaction : senderPendingTransactions) {
        if (pendingTransaction.getTransaction().getType().supportsBlob()) {
          continue goToNextSender;
        }

        final int txSize = pendingTransaction.getTransaction().getSizeForBlockInclusion();

        // this can be optimized checking if the remaining space could fit a smaller tx
        if (totalBytes + txSize > maxBytes) {
          maxSizeReached = true;
          break;
        }

        selected.add(pendingTransaction);
        totalBytes += txSize;
      }

      if (maxSizeReached) {
        break;
      }
    }

    LOG.atDebug()
        .setMessage(
            "IL selector: selected {} transactions ({} bytes) from {} candidates for parent {}")
        .addArgument(selected.size())
        .addArgument(totalBytes)
        .addArgument(
            () -> pendingTransactionsBySender.stream().map(List::size).reduce(0, Integer::sum))
        .addArgument(parentHash)
        .log();

    return selected;
  }
}
