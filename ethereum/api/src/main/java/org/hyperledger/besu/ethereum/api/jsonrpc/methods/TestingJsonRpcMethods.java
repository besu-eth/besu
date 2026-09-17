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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing.TestingBuildBlockV1;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.Map;

/**
 * Testing JSON-RPC methods. These methods are intended for testing environments only and should not
 * be exposed on public-facing RPC APIs.
 */
public class TestingJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final MiningConfiguration miningConfiguration;
  private final TransactionPool transactionPool;
  private final EthScheduler ethScheduler;

  public TestingJsonRpcMethods(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final MiningConfiguration miningConfiguration,
      final TransactionPool transactionPool,
      final EthScheduler ethScheduler) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.miningConfiguration = miningConfiguration;
    this.transactionPool = transactionPool;
    this.ethScheduler = ethScheduler;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.TESTING.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    return mapOf(
        new TestingBuildBlockV1(
            protocolContext, protocolSchedule, miningConfiguration, transactionPool, ethScheduler));
  }
}
