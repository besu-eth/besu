/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.pending;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.JsonRpcResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.TransactionBaseResult;
import org.hyperledger.besu.ethereum.core.Transaction;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
  "accessList",
  "authorizationList",
  "blockHash",
  "blockNumber",
  "blockTimestamp",
  "chainId",
  "from",
  "gas",
  "gasPrice",
  "maxPriorityFeePerGas",
  "maxFeePerGas",
  "maxFeePerBlobGas",
  "hash",
  "input",
  "nonce",
  "to",
  "transactionIndex",
  "type",
  "value",
  "yParity",
  "v",
  "r",
  "s",
  "blobVersionedHashes"
})
public class PendingTransactionDetailResult extends TransactionBaseResult implements JsonRpcResult {

  public PendingTransactionDetailResult(final Transaction tx) {
    super(tx);
  }
}
