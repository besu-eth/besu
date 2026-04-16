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

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.AMSTERDAM;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;

public class EngineNewPayloadV5 extends ExecutionEngineJsonRpcMethod {

  private static final NewPayloadRules RULES =
      NewPayloadRules.builder()
          .forkWindow(ForkWindow.from(AMSTERDAM))
          .blobGasUsed(FieldPresence.REQUIRED)
          .excessBlobGas(FieldPresence.REQUIRED)
          .versionedHashes(FieldPresence.REQUIRED)
          .parentBeaconBlockRoot(FieldPresence.REQUIRED)
          .executionRequests(FieldPresence.REQUIRED)
          .slotNumber(FieldPresence.REQUIRED)
          .blobValidation(BlobValidationMode.STANDARD)
          .blockAccessListExtractor(EngineNewPayloadV5::decodeBlockAccessList)
          .build();

  private final NewPayloadProcessor processor;

  public EngineNewPayloadV5(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener,
      final NewPayloadProcessor processor) {
    super(vertx, protocolSchedule, protocolContext, engineCallListener);
    this.processor = processor;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V5.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();
    return processor.process(requestContext, RULES);
  }

  private static Optional<BlockAccessList> decodeBlockAccessList(
      final EnginePayloadParameter payloadParameter) throws InvalidBlockAccessListException {
    final String blockAccessList = payloadParameter.getBlockAccessList();
    if (blockAccessList == null || blockAccessList.isEmpty()) {
      throw new InvalidBlockAccessListException("Missing block access list field");
    }
    final Bytes encoded;
    try {
      encoded = Bytes.fromHexString(blockAccessList);
    } catch (final IllegalArgumentException e) {
      throw new InvalidBlockAccessListException("Invalid block access list encoding", e);
    }
    try {
      return Optional.of(BlockAccessListDecoder.decode(new BytesValueRLPInput(encoded, false)));
    } catch (final RuntimeException e) {
      throw new InvalidBlockAccessListException("Invalid block access list encoding", e);
    }
  }
}
