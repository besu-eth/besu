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

import static java.util.stream.Collectors.toList;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.consensus.merge.blockcreation.PreparePayloadArgsBuilder;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePreparePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EnginePreparePayloadResult;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;
import java.util.Optional;

import graphql.VisibleForTesting;
import io.vertx.core.Vertx;

public class EnginePreparePayloadDebug extends ExecutionEngineJsonRpcMethod {
  private final MergeMiningCoordinator mergeCoordinator;

  public EnginePreparePayloadDebug(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final EngineCallListener engineCallListener,
      final MergeMiningCoordinator mergeCoordinator) {
    super(vertx, protocolContext, engineCallListener);
    this.mergeCoordinator = mergeCoordinator;
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_PREPARE_PAYLOAD_DEBUG.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    final EnginePreparePayloadParameter enginePreparePayloadParameter;
    try {
      enginePreparePayloadParameter =
          requestContext
              .getOptionalParameter(0, EnginePreparePayloadParameter.class)
              .orElse(
                  new EnginePreparePayloadParameter(
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty(),
                      Optional.empty()));
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid engine prepare payload parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_PREPARE_PAYLOAD_PARAMS,
          e);
    }

    final var requestId = requestContext.getRequest().getId();

    if (mergeContext.get().isSyncing()) {
      return new JsonRpcSuccessResponse(requestId, new EnginePreparePayloadResult(SYNCING, null));
    }

    return generatePayload(enginePreparePayloadParameter)
        .<JsonRpcResponse>map(
            payloadIdentifier ->
                new JsonRpcSuccessResponse(
                    requestId, new EnginePreparePayloadResult(VALID, payloadIdentifier)))
        .orElseGet(
            () ->
                new JsonRpcErrorResponse(
                    requestId, RpcErrorType.INVALID_ENGINE_PREPARE_PAYLOAD_PARAMS));
  }

  @VisibleForTesting
  Optional<PayloadIdentifier> generatePayload(final EnginePreparePayloadParameter param) {
    final List<Withdrawal> withdrawals =
        param.getWithdrawals().stream().map(WithdrawalParameter::toWithdrawal).collect(toList());

    return param
        .getParentHash()
        .map(header -> protocolContext.getBlockchain().getBlockHeader(header))
        .orElseGet(() -> Optional.of(protocolContext.getBlockchain().getChainHeadHeader()))
        .map(
            parentHeader ->
                mergeCoordinator.preparePayload(
                    new PreparePayloadArgsBuilder()
                        .parentHeader(parentHeader)
                        .timestamp(param.getTimestamp().orElse(parentHeader.getTimestamp() + 1L))
                        .prevRandao(param.getPrevRandao())
                        .feeRecipient(param.getFeeRecipient())
                        .withdrawals(Optional.of(withdrawals))
                        .parentBeaconBlockRoot(param.getParentBeaconBlockRoot())
                        .slotNumber(Optional.empty())
                        .build()));
  }
}
