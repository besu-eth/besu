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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.RequestValidatorProvider.getRequestsValidator;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.FAIL_ON_UNKNOWN_BUT_NULL;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;

public sealed class EngineNewPayloadV4<
        EP extends ExecutionPayloadV3, NPRP extends NewPayloadRequestParametersV3<? extends EP>>
    extends EngineNewPayloadV3<EP, NPRP> permits EngineNewPayloadV5 {

  public EngineNewPayloadV4(
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
    return RpcMethod.ENGINE_NEW_PAYLOAD_V4.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected NPRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final NewPayloadRequestParametersV2<? extends EP> requestParameters =
        super.readRequestParameters(requestContext);
    final List<Request> executionRequests;
    try {
      executionRequests =
          requestContext.getRequiredList(3, Request.class, FAIL_ON_UNKNOWN_BUT_NULL);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid execution request parameters (index 3)",
          RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS,
          e);
    }
    return (NPRP) new NewPayloadRequestParametersV3<>(requestParameters, executionRequests);
  }

  @Override
  protected int getNumberOfParameters() {
    return 4;
  }

  @Override
  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    super.setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    blockHeaderBuilder.requestsHash(
        BodyValidation.requestsHash(requestParameters.executionRequests()));
  }

  @Override
  protected ValidationResult<RpcErrorType> validateNewBlock(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final BlockHeader parentHeader,
      final NPRP requestParameters) {
    final ValidationResult<RpcErrorType> result =
        super.validateNewBlock(newBlock, protocolSpec, parentHeader, requestParameters);
    return result.isValid() ? validateExecutionPayloadV4(requestParameters) : result;
  }

  private ValidationResult<RpcErrorType> validateExecutionPayloadV4(final NPRP requestParameters) {
    final var payloadParameter = requestParameters.payloadParameter();
    if (!getRequestsValidator(
            protocolSchedule.get(),
            payloadParameter.getTimestamp(),
            payloadParameter.getBlockNumber())
        .validate(Optional.of(requestParameters.executionRequests()))) {
      return ValidationResult.invalid(RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
    }
    return ValidationResult.valid();
  }

  @Override
  protected JsonRpcResponse processParametersParsingException(
      final Object reqId, final InvalidJsonRpcRequestException e) {

    if (e.getRpcErrorType() == RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS) {
      return new JsonRpcErrorResponse(
          reqId,
          ValidationResult.invalid(
              RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS,
              "Failed to decode execution requests parameter (" + e.getMessage() + ")"));
    }
    return super.processParametersParsingException(reqId, e);
  }
}
