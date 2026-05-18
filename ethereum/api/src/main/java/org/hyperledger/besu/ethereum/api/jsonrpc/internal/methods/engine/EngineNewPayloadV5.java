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

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;

public final class EngineNewPayloadV5 extends EngineNewPayloadV4 {

  public EngineNewPayloadV5(
      final Vertx vertx,
      final ProtocolSchedule timestampSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final EngineCallListener engineCallListener,
      final MetricsSystem metricsSystem,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(
        vertx,
        timestampSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        metricsSystem,
        minSupportedFork,
        firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V5.getMethodName();
  }

  @Override
  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV4.class;
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(
      final NewPayloadRequestParametersV1 requestParameters) {
    final ValidationResult<RpcErrorType> result = super.validateParameters(requestParameters);
    if (!result.isValid()) {
      return result;
    }
    final ExecutionPayloadV4 executionPayloadV4 =
        (ExecutionPayloadV4) requestParameters.payloadParameter();
    if (executionPayloadV4.getSlotNumber() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_SLOT_NUMBER_PARAMS, "Missing slot number field");
    }
    return ValidationResult.valid();
  }

  @Override
  protected VersionSpecificPayloadData createVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    final V4PayloadData v4PayloadData =
        (V4PayloadData) super.createVersionSpecificPayloadData(requestParameters);
    return new V5PayloadData(v4PayloadData, decodeBlockAccessList(requestParameters));
  }

  private Optional<BlockAccessList> decodeBlockAccessList(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    final ExecutionPayloadV4 executionPayloadV4 =
        (ExecutionPayloadV4) requestParameters.payloadParameter();
    final String blockAccessList = executionPayloadV4.getBlockAccessList();
    if (blockAccessList == null || blockAccessList.isEmpty()) {
      throw InvalidVersionSpecificPayloadException.invalidPayload(
          "Missing block access list field");
    }
    final Bytes encoded;
    try {
      encoded = Bytes.fromHexString(blockAccessList);
    } catch (final IllegalArgumentException e) {
      throw InvalidVersionSpecificPayloadException.invalidPayload(
          "Invalid block access list encoding");
    }
    try {
      return Optional.of(BlockAccessListDecoder.decode(new BytesValueRLPInput(encoded, false)));
    } catch (final RuntimeException e) {
      throw InvalidVersionSpecificPayloadException.invalidPayload(
          "Invalid block access list encoding");
    }
  }

  @Override
  protected void setVersionSpecificBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder,
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.setVersionSpecificBlockHeaderFields(
        blockHeaderBuilder, requestParameters, versionSpecificPayloadData);
    final ExecutionPayloadV4 payloadParameter =
        (ExecutionPayloadV4) requestParameters.payloadParameter();
    final V5PayloadData v5PayloadData = (V5PayloadData) versionSpecificPayloadData;
    blockHeaderBuilder
        .balHash(v5PayloadData.maybeBlockAccessList().map(BodyValidation::balHash).orElse(null))
        .slotNumber(payloadParameter.getSlotNumber());
  }

  @Override
  protected BlockProcessingResult rememberBlock(
      final Block block, final VersionSpecificPayloadData versionSpecificPayloadData) {
    final V5PayloadData v5PayloadData = (V5PayloadData) versionSpecificPayloadData;
    return getMergeCoordinator().rememberBlock(block, v5PayloadData.maybeBlockAccessList());
  }

  protected static class V5PayloadData extends V4PayloadData {
    private final Optional<BlockAccessList> maybeBlockAccessList;

    protected V5PayloadData(
        final V4PayloadData v4PayloadData, final Optional<BlockAccessList> maybeBlockAccessList) {
      super(v4PayloadData, v4PayloadData.maybeRequests());
      this.maybeBlockAccessList = maybeBlockAccessList;
    }

    protected Optional<BlockAccessList> maybeBlockAccessList() {
      return maybeBlockAccessList;
    }
  }
}
