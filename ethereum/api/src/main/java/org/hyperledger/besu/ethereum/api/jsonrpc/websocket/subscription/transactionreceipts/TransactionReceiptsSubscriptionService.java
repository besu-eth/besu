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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.transactionreceipts;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockReceiptsResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptRootResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionReceiptStatusResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.request.SubscriptionType;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.api.query.TransactionReceiptWithMetadata;
import org.hyperledger.besu.ethereum.chain.BlockAddedEvent;
import org.hyperledger.besu.ethereum.chain.BlockAddedObserver;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.TransactionReceiptType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Suppliers;

public class TransactionReceiptsSubscriptionService implements BlockAddedObserver {

  private final SubscriptionManager subscriptionManager;
  private final BlockchainQueries blockchainQueries;
  private final ProtocolSchedule protocolSchedule;

  public TransactionReceiptsSubscriptionService(
      final SubscriptionManager subscriptionManager,
      final BlockchainQueries blockchainQueries,
      final ProtocolSchedule protocolSchedule) {
    this.subscriptionManager = subscriptionManager;
    this.blockchainQueries = blockchainQueries;
    this.protocolSchedule = protocolSchedule;
  }

  @Override
  public void onBlockAdded(final BlockAddedEvent event) {
    if (event.isNewCanonicalHead()) {
      final List<Hash> blockHashes = new ArrayList<>();
      BlockHeader blockPtr = event.getHeader();

      while (!blockPtr.getHash().equals(event.getCommonAncestorHash())) {
        blockHashes.add(blockPtr.getHash());
        blockPtr =
            blockchainQueries
                .getBlockchain()
                .getBlockHeader(blockPtr.getParentHash())
                .orElseThrow(
                    () -> new IllegalStateException("The block was on an orphaned chain."));
      }

      Collections.reverse(blockHashes);
      blockHashes.forEach(this::notifySubscribers);
    }
  }

  private void notifySubscribers(final Hash blockHash) {
    subscriptionManager.notifySubscribersOnWorkerThread(
        SubscriptionType.TRANSACTION_RECEIPTS,
        TransactionReceiptsSubscription.class,
        subscribers -> {
          final var receiptList = Suppliers.memoize(() -> buildReceiptResults(blockHash));

          for (final TransactionReceiptsSubscription subscription : subscribers) {
            final List<TransactionReceiptResult> allReceipts = receiptList.get();
            if (allReceipts.isEmpty()) {
              continue;
            }

            final List<TransactionReceiptResult> filtered;
            if (subscription.hasFilter()) {
              filtered =
                  allReceipts.stream()
                      .filter(
                          r ->
                              subscription
                                  .getTransactionHashesFilter()
                                  .contains(Hash.fromHexString(r.getTransactionHash())))
                      .toList();
              if (filtered.isEmpty()) {
                continue;
              }
            } else {
              filtered = allReceipts;
            }

            subscriptionManager.sendMessage(
                subscription.getSubscriptionId(), new BlockReceiptsResult(filtered));
          }
        });
  }

  private List<TransactionReceiptResult> buildReceiptResults(final Hash blockHash) {
    return blockchainQueries
        .transactionReceiptsByBlockHash(blockHash, protocolSchedule)
        .orElse(Collections.emptyList())
        .stream()
        .map(this::toReceiptResult)
        .toList();
  }

  private TransactionReceiptResult toReceiptResult(
      final TransactionReceiptWithMetadata receiptWithMetadata) {
    if (receiptWithMetadata.getReceipt().getTransactionReceiptType()
        == TransactionReceiptType.ROOT) {
      return new TransactionReceiptRootResult(receiptWithMetadata);
    }
    return new TransactionReceiptStatusResult(receiptWithMetadata);
  }
}
