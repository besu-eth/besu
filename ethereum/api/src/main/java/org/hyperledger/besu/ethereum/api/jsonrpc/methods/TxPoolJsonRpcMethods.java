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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolBesuPendingTransactions;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolBesuStatistics;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolBesuTransactions;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolContent;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolContentFrom;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolInspect;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.TxPoolStatus;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;

import java.util.Map;

public class TxPoolJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final TransactionPool transactionPool;

  public TxPoolJsonRpcMethods(final TransactionPool transactionPool) {
    this.transactionPool = transactionPool;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.TXPOOL.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    return mapOf(
        new TxPoolBesuTransactions(transactionPool),
        new TxPoolBesuPendingTransactions(transactionPool),
        new TxPoolBesuStatistics(transactionPool),
        new TxPoolStatus(transactionPool),
        new TxPoolContentFrom(transactionPool),
        new TxPoolContent(transactionPool),
        new TxPoolInspect(transactionPool));
  }
}
