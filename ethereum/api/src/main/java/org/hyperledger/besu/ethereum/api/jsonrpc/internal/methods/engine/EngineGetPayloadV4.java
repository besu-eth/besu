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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.consensus.merge.PayloadWrapper;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV4;
import org.hyperledger.besu.ethereum.core.Request;

import java.util.Comparator;
import java.util.List;

public sealed class EngineGetPayloadV4 extends EngineGetPayloadV3 permits EngineGetPayloadV5 {

  public EngineGetPayloadV4(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V4.getMethodName();
  }

  @Override
  protected Object createResponse(final PayloadWrapper payload) {
    return new EngineGetPayloadResultV4(
        createExecutionPayload(payload),
        payload.blockValue(),
        createBlobsBundle(payload.blockWithReceipts().getBlock().getBody().getTransactions()),
        prepareRequests(payload));
  }

  protected List<Request> prepareRequests(final PayloadWrapper payload) {
    return payload
        .requests()
        .map(
            rqs ->
                rqs.stream()
                    .sorted(Comparator.comparing(Request::getType))
                    .filter(r -> !r.getData().isEmpty())
                    .toList())
        .orElse(null);
  }
}
