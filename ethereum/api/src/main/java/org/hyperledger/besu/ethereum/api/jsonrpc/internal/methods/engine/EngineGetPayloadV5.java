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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobsBundleV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV5;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;

public sealed class EngineGetPayloadV5 extends EngineGetPayloadV4 permits EngineGetPayloadV6 {

  public EngineGetPayloadV5(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName();
  }

  @Override
  protected Object createResponse(final PayloadWrapper payload) {
    return new EngineGetPayloadResultV5(
        createExecutionPayload(payload),
        payload.blockValue(),
        createBlobsBundle(payload.blockWithReceipts().getBlock().getBody().getTransactions()),
        prepareRequests(payload));
  }

  @Override
  protected BlobsBundleV2 createBlobsBundle(final List<Transaction> transactions) {
    return new BlobsBundleV2(transactions);
  }
}
