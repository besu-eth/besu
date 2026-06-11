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
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV6;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import io.vertx.core.Vertx;

public final class EngineGetPayloadV6 extends EngineGetPayloadV5 {

  public EngineGetPayloadV6(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final MergeMiningCoordinator mergeMiningCoordinator,
      final BlockResultFactory blockResultFactory,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(
        protocolSchedule,
        protocolContext,
        vertx,
        engineCallListener,
        mergeMiningCoordinator,
        blockResultFactory,
        minSupportedFork,
        firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V6.getMethodName();
  }

  @Override
  protected Object createResponse(final PayloadWrapper payload) {
    return new EngineGetPayloadResultV6(
        createExecutionPayload(payload),
        payload.blockValue(),
        createBlobsBundle(payload.blockWithReceipts().getBlock().getBody().getTransactions()),
        prepareRequests(payload));
  }

  @Override
  protected ExecutionPayloadV4 createExecutionPayload(final PayloadWrapper payload) {
    final Block block = payload.blockWithReceipts().getBlock();

    return new ExecutionPayloadV4(
        block.getHeader(),
        block.getBody().getTransactions(),
        getWithdrawals(block.getBody()),
        getBlockAccessList(payload));
  }

  private BlockAccessList getBlockAccessList(final PayloadWrapper payload) {
    return payload
        .blockAccessList()
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Block access list must be present after Amsterdam hardfork"));
  }
}
