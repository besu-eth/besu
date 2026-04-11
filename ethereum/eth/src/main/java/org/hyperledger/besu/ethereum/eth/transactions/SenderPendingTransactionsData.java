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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.datatypes.Address;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A list of pending transactions of a specific sender, ordered by nonce asc
 *
 * @param sender the sender
 * @param nonce the expected nonce for the sender as seen by the txpool
 * @param pendingTransactions the list of pending transactions order by nonce asc
 */
public record SenderPendingTransactionsData(
    Address sender, long nonce, List<PendingTransaction> pendingTransactions) {

  public static SenderPendingTransactionsData empty(final Address sender) {
    return new SenderPendingTransactionsData(sender, 0L, List.of());
  }

  @Override
  public String toString() {
    return "Sender "
        + sender
        + " has nonce "
        + nonce
        + " and "
        + pendingTransactions.size()
        + " pending transactions "
        + pendingTransactions.stream()
            .map(PendingTransaction::toTraceLog)
            .collect(Collectors.joining(",", "[", "]"));
  }
}
