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

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;

public sealed class EngineNewPayloadV4 extends EngineNewPayloadV3 permits EngineNewPayloadV5 {

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
  protected NewPayloadRequestParametersV3 readRequestParameters(
      final JsonRpcRequestContext requestContext) {
    final NewPayloadRequestParametersV2 requestParameters =
        super.readRequestParameters(requestContext);
    final Optional<List<String>> executionRequests;
    try {
      executionRequests = requestContext.getOptionalList(3, String.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid execution request parameters (index 3)",
          RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS,
          e);
    }
    return new NewPayloadRequestParametersV3(requestParameters, executionRequests);
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(
      final NewPayloadRequestParametersV1 requestParameters) {
    final ValidationResult<RpcErrorType> result = super.validateParameters(requestParameters);
    if (!result.isValid()) {
      return result;
    }
    final NewPayloadRequestParametersV3 requestParametersV3 =
        (NewPayloadRequestParametersV3) requestParameters;
    final Optional<List<String>> executionRequests = requestParametersV3.executionRequests();
    if (executionRequests.isEmpty()) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS, "Missing execution requests field");
    }
    return ValidationResult.valid();
  }

  @Override
  protected VersionSpecificPayloadData createVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    final V3PayloadData v3PayloadData =
        (V3PayloadData) super.createVersionSpecificPayloadData(requestParameters);
    final NewPayloadRequestParametersV3 requestParametersV3 =
        (NewPayloadRequestParametersV3) requestParameters;
    try {
      return new V4PayloadData(
          v3PayloadData, parseRequests(requestParametersV3.executionRequests()));
    } catch (final RuntimeException e) {
      throw InvalidVersionSpecificPayloadException.jsonRpcError(
          RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
    }
  }

  @Override
  protected ValidationResult<RpcErrorType> validateVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    final ValidationResult<RpcErrorType> result =
        super.validateVersionSpecificPayloadData(requestParameters, versionSpecificPayloadData);
    if (!result.isValid()) {
      return result;
    }
    final V4PayloadData v4PayloadData = (V4PayloadData) versionSpecificPayloadData;
    final var payloadParameter = requestParameters.payloadParameter();
    if (!getRequestsValidator(
            protocolSchedule.get(),
            payloadParameter.getTimestamp(),
            payloadParameter.getBlockNumber())
        .validate(v4PayloadData.maybeRequests())) {
      return ValidationResult.invalid(RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS);
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setVersionSpecificBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder,
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.setVersionSpecificBlockHeaderFields(
        blockHeaderBuilder, requestParameters, versionSpecificPayloadData);
    final V4PayloadData v4PayloadData = (V4PayloadData) versionSpecificPayloadData;
    blockHeaderBuilder.requestsHash(
        v4PayloadData.maybeRequests().map(BodyValidation::requestsHash).orElse(null));
  }

  protected Optional<List<Request>> parseRequests(final Optional<List<String>> maybeRequestsParam) {
    if (maybeRequestsParam.isEmpty()) {
      return Optional.empty();
    }
    return maybeRequestsParam.map(
        requests ->
            requests.stream()
                .map(
                    s -> {
                      final Bytes request = Bytes.fromHexString(s);
                      final Bytes requestData = request.slice(1);
                      if (requestData.isEmpty()) {
                        throw new IllegalArgumentException("Request data cannot be empty");
                      }
                      return new Request(RequestType.of(request.get(0)), requestData);
                    })
                .toList());
  }

  protected static class V4PayloadData extends V3PayloadData {
    private final Optional<List<Request>> maybeRequests;

    protected V4PayloadData(
        final V3PayloadData v3PayloadData, final Optional<List<Request>> maybeRequests) {
      super(v3PayloadData, v3PayloadData.maybeVersionedHashes());
      this.maybeRequests = maybeRequests;
    }

    protected Optional<List<Request>> maybeRequests() {
      return maybeRequests;
    }
  }
}
