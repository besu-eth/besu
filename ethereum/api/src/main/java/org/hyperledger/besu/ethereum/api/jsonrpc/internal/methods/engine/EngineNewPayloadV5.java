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

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV4;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EngineNewPayloadV5<
        EP extends ExecutionPayloadV4, NPRP extends NewPayloadRequestParametersV3<? extends EP>>
    extends EngineNewPayloadV4<EP, NPRP> {

  private static final Logger LOG = LoggerFactory.getLogger(EngineNewPayloadV5.class);

  public EngineNewPayloadV5(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  protected Logger logger() {
    return LOG;
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
  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    super.setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    blockHeaderBuilder
        .balHash(BodyValidation.balHash(requestParameters.payloadParameter().getBlockAccessList()))
        .slotNumber(requestParameters.payloadParameter().getSlotNumber());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(final NPRP requestParameters) {
    final ValidationResult<RpcErrorType> result = super.validateParameters(requestParameters);
    return result.isValid() ? validateParametersV5(requestParameters) : result;
  }

  private ValidationResult<RpcErrorType> validateParametersV5(
      final NewPayloadRequestParametersV3<? extends EP> requestParameters) {
    final ExecutionPayloadV4 executionPayloadV4 = requestParameters.payloadParameter();
    if (executionPayloadV4.getBlockAccessList() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_BLOCK_ACCESS_LIST_PARAMS, "Missing block access list field");
    }
    if (executionPayloadV4.getSlotNumber() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_SLOT_NUMBER_PARAMS, "Missing slot number field");
    }
    return ValidationResult.valid();
  }

  @Override
  protected BlockProcessingResult rememberBlock(final Block block, final EP executionPayload) {
    return mergeCoordinator.rememberBlock(
        block, Optional.of(executionPayload.getBlockAccessList()));
  }

  @Override
  protected JsonRpcResponse processParametersParsingException(
      final Object reqId, final InvalidRequestParametersException e) {
    final Optional<JsonMappingException> maybeFieldEx =
        extractCauseByType(e, JsonMappingException.class);

    // specific invalid field with custom error response
    if (maybeFieldEx.isPresent()) {
      final JsonMappingException fieldEx = maybeFieldEx.get();
      final Optional<String> maybeJsonPath = extractJsonPath(fieldEx);
      if (maybeJsonPath.isPresent()) {
        final String jsonPath = maybeJsonPath.get();

        if (jsonPath.equals("blockAccessList")) {
          return new JsonRpcErrorResponse(
              reqId,
              ValidationResult.invalid(
                  RpcErrorType.INVALID_BLOCK_ACCESS_LIST_PARAMS,
                  "Failed to decode block access list payload parameter ("
                      + fieldEx.getOriginalMessage()
                      + ")"));
        }
      }
    }

    return super.processParametersParsingException(reqId, e);
  }
}
