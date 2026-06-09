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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV2;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;

public sealed class EngineGetPayloadV2 extends EngineGetPayloadV1 permits EngineGetPayloadV3 {
  private final Optional<Long> shanghaiTimestamp;

  public EngineGetPayloadV2(
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
    shanghaiTimestamp = protocolSchedule.milestoneFor(HardforkId.MainnetHardforkId.SHANGHAI);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_V2.getMethodName();
  }

  @Override
  protected Object createResponse(final PayloadWrapper payload) {
    return new EngineGetPayloadResultV2(createExecutionPayload(payload), payload.blockValue());
  }

  @Override
  protected ExecutionPayloadV1 createExecutionPayload(final PayloadWrapper payload) {
    final Block block = payload.blockWithReceipts().getBlock();
    final BlockBody blockBody = block.getBody();

    // ExecutionPayloadV1 MUST be returned if the payload timestamp is lower than the Shanghai
    // timestamp
    // ExecutionPayloadV2 MUST be returned if the payload timestamp is greater or equal to the
    // Shanghai timestamp
    final long timestamp = block.getHeader().getTimestamp();
    if (shanghaiTimestamp.isEmpty() || timestamp < shanghaiTimestamp.get()) {
      if (blockBody.getWithdrawals().isPresent()) {
        throw new IllegalStateException(
            "Withdrawals should not be present before Shanghai hardfork");
      }
      return new ExecutionPayloadV1(block.getHeader(), blockBody.getTransactions());
    } else {
      return new ExecutionPayloadV2(
          block.getHeader(), blockBody.getTransactions(), getWithdrawals(blockBody));
    }
  }

  protected List<Withdrawal> getWithdrawals(final BlockBody blockBody) {
    return blockBody
        .getWithdrawals()
        .orElseThrow(
            () ->
                new IllegalStateException("Withdrawals should be present after Shanghai hardfork"));
  }

  @Override
  protected String versionSpecificLogInfo(final Block block) {
    return block
        .getBody()
        .getWithdrawals()
        .map(withdrawals -> String.format(" | %d ws", withdrawals.size()))
        .orElseGet(() -> super.versionSpecificLogInfo(block));
  }
}
