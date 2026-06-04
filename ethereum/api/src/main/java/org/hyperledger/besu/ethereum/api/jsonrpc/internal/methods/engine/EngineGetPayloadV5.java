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
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;

import io.vertx.core.Vertx;

public sealed class EngineGetPayloadV5 extends EngineGetPayloadV4 permits EngineGetPayloadV6 {

  public EngineGetPayloadV5(
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
    return RpcMethod.ENGINE_GET_PAYLOAD_V5.getMethodName();
  }

  @Override
  protected Object createResponsePayload(final PayloadWrapper payload) {
    return blockResultFactory.payloadTransactionCompleteV5(payload);
  }
}
