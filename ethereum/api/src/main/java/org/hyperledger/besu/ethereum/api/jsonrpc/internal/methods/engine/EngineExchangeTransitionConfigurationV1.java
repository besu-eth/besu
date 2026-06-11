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

import static org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod.ENGINE_EXCHANGE_TRANSITION_CONFIGURATION;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.TransitionConfigurationV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineExchangeTransitionConfigurationResult;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;
import java.util.function.Supplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineExchangeTransitionConfigurationV1 extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG =
      LoggerFactory.getLogger(EngineExchangeTransitionConfigurationV1.class);

  // use (2^256 - 2^10) if engine is enabled in the absence of a TTD configuration
  static final Difficulty FALLBACK_TTD_DEFAULT =
      Difficulty.fromHexString(
          "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffc00");

  private static final Supplier<ObjectMapper> mapperSupplier = Suppliers.memoize(ObjectMapper::new);

  private final Optional<Long> minForkTimestamp;
  private final Optional<Long> maxForkTimestamp;

  private final HardforkId minSupportedFork;
  private final HardforkId firstUnsupportedFork;

  public EngineExchangeTransitionConfigurationV1(
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final Vertx vertx,
      final EngineCallListener engineCallListener,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(protocolSchedule, protocolContext, vertx, engineCallListener);
    this.minSupportedFork = minSupportedFork;
    this.firstUnsupportedFork = firstUnsupportedFork;
    this.minForkTimestamp =
        minSupportedFork != null
            ? protocolSchedule.milestoneFor(minSupportedFork)
            : Optional.empty();
    this.maxForkTimestamp =
        firstUnsupportedFork != null
            ? protocolSchedule.milestoneFor(firstUnsupportedFork)
            : Optional.empty();
  }

  @Override
  public String getName() {
    return ENGINE_EXCHANGE_TRANSITION_CONFIGURATION.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    engineCallListener.executionEngineCalled();

    final TransitionConfigurationV1 remoteTransitionConfiguration;
    try {
      remoteTransitionConfiguration =
          requestContext.getRequiredParameter(0, TransitionConfigurationV1.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid engine exchange transition configuration parameters (index 0)",
          RpcErrorType.INVALID_ENGINE_EXCHANGE_TRANSITION_CONFIGURATION_PARAMS,
          e);
    }
    final Object reqId = requestContext.getRequest().getId();

    LOG.atTrace()
        .setMessage("received transitionConfiguration: {}")
        .addArgument(
            () -> {
              try {
                return mapperSupplier.get().writeValueAsString(remoteTransitionConfiguration);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            })
        .log();

    final long timestamp = protocolContext.getBlockchain().getChainHeadHeader().getTimestamp();
    ValidationResult<RpcErrorType> forkValidationResult = validateForkRange(timestamp);
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), forkValidationResult);
    }

    final Optional<BlockHeader> maybeTerminalPoWBlockHeader =
        mergeContextOptional.flatMap(MergeContext::getTerminalPoWBlock);

    final EngineExchangeTransitionConfigurationResult localTransitionConfiguration =
        new EngineExchangeTransitionConfigurationResult(
            mergeContextOptional
                .map(MergeContext::getTerminalTotalDifficulty)
                .orElse(FALLBACK_TTD_DEFAULT),
            maybeTerminalPoWBlockHeader.map(BlockHeader::getHash).orElse(Hash.ZERO),
            maybeTerminalPoWBlockHeader.map(BlockHeader::getNumber).orElse(0L));

    if (!localTransitionConfiguration
        .getTerminalTotalDifficulty()
        .equals(remoteTransitionConfiguration.getTerminalTotalDifficulty())) {
      LOG.debug(
          "Configured terminal total difficulty {} does not match value of consensus client {}",
          localTransitionConfiguration.getTerminalTotalDifficulty(),
          remoteTransitionConfiguration.getTerminalTotalDifficulty());
    }

    if (!localTransitionConfiguration
        .getTerminalBlockHash()
        .equals(remoteTransitionConfiguration.getTerminalBlockHash())) {
      LOG.debug(
          "Configured terminal block hash {} does not match value of consensus client {}",
          localTransitionConfiguration.getTerminalBlockHash(),
          remoteTransitionConfiguration.getTerminalBlockHash());
    }

    if (localTransitionConfiguration.getTerminalBlockNumber()
        != remoteTransitionConfiguration.getTerminalBlockNumber()) {
      LOG.debug(
          "Configured terminal block number {} does not match value of consensus client {}",
          localTransitionConfiguration.getTerminalBlockNumber(),
          remoteTransitionConfiguration.getTerminalBlockNumber());
    }

    return respondWith(reqId, localTransitionConfiguration);
  }

  private JsonRpcResponse respondWith(
      final Object requestId,
      final EngineExchangeTransitionConfigurationResult transitionConfiguration) {
    return new JsonRpcSuccessResponse(requestId, transitionConfiguration);
  }

  private ValidationResult<RpcErrorType> validateForkRange(final long timestamp) {
    return ForkSupportHelper.validateForkSupported(
        minSupportedFork, minForkTimestamp, firstUnsupportedFork, maxForkTimestamp, timestamp);
  }
}
