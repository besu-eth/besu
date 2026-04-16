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

import static java.util.stream.Collectors.toList;

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.RequestType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.mainnet.BodyValidation;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Stateless helpers that parse an {@code engine_newPayload} JSON-RPC request into typed domain
 * objects and build the resulting {@link BlockHeader}.
 *
 * <p>Each method is a pure function of its inputs; the class holds no state and cannot be
 * instantiated.
 */
public final class NewPayloadParamsParser {

  private static final Hash OMMERS_HASH_CONSTANT = Hash.EMPTY_LIST_HASH;
  private static final BlockHeaderFunctions HEADER_FUNCTIONS = new MainnetBlockHeaderFunctions();

  private NewPayloadParamsParser() {}

  public static EnginePayloadParameter parseBlockParam(final JsonRpcRequestContext ctx) {
    try {
      return ctx.getRequiredParameter(0, EnginePayloadParameter.class);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid engine payload parameter (index 0)",
          RpcErrorType.INVALID_ENGINE_NEW_PAYLOAD_PARAMS,
          e);
    }
  }

  public static Optional<List<String>> parseVersionedHashParam(final JsonRpcRequestContext ctx) {
    try {
      return ctx.getOptionalList(1, String.class);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid versioned hash parameters (index 1)",
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          e);
    }
  }

  public static Optional<String> parseParentBeaconRootParam(final JsonRpcRequestContext ctx) {
    try {
      return ctx.getOptionalParameter(2, String.class);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid parent beacon block root parameters (index 2)",
          RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
          e);
    }
  }

  public static Optional<List<String>> parseRequestsParam(final JsonRpcRequestContext ctx) {
    try {
      return ctx.getOptionalList(3, String.class);
    } catch (final JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid execution request parameters (index 3)",
          RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS,
          e);
    }
  }

  public static Optional<List<VersionedHash>> decodeVersionedHashes(
      final Optional<List<String>> maybeVersionedHashParam) {
    return maybeVersionedHashParam.map(
        versionedHashes ->
            versionedHashes.stream()
                .map(Bytes32::fromHexString)
                .map(
                    hash -> {
                      try {
                        return new VersionedHash(hash);
                      } catch (final InvalidParameterException e) {
                        throw new RuntimeException(e);
                      }
                    })
                .collect(Collectors.toList()));
  }

  public static Optional<List<Withdrawal>> extractWithdrawals(
      final EnginePayloadParameter blockParam) {
    return Optional.ofNullable(blockParam.getWithdrawals())
        .map(ws -> ws.stream().map(WithdrawalParameter::toWithdrawal).collect(toList()));
  }

  public static Optional<List<Request>> decodeRequests(
      final Optional<List<String>> maybeRequestsParam) {
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
                .collect(Collectors.toList()));
  }

  public static Optional<BlockAccessList> extractBlockAccessList(
      final NewPayloadRules rules, final EnginePayloadParameter blockParam)
      throws InvalidBlockAccessListException {
    return rules.blockAccessListExtractor().extract(blockParam);
  }

  public static List<Transaction> decodeTransactions(final EnginePayloadParameter blockParam) {
    return blockParam.getTransactions().stream()
        .map(Bytes::fromHexString)
        .map(in -> TransactionDecoder.decodeOpaqueBytes(in, EncodingContext.BLOCK_BODY))
        .toList();
  }

  public static BlockHeader buildHeader(
      final EnginePayloadParameter blockParam,
      final List<Transaction> transactions,
      final Optional<List<Withdrawal>> withdrawals,
      final Optional<List<Request>> requests,
      final Optional<BlockAccessList> blockAccessList,
      final Optional<Bytes32> parentBeaconRoot) {
    return new BlockHeader(
        blockParam.getParentHash(),
        OMMERS_HASH_CONSTANT,
        blockParam.getFeeRecipient(),
        blockParam.getStateRoot(),
        BodyValidation.transactionsRoot(transactions),
        blockParam.getReceiptsRoot(),
        blockParam.getLogsBloom(),
        Difficulty.ZERO,
        blockParam.getBlockNumber(),
        blockParam.getGasLimit(),
        blockParam.getGasUsed(),
        blockParam.getTimestamp(),
        Bytes.fromHexString(blockParam.getExtraData()),
        blockParam.getBaseFeePerGas(),
        blockParam.getPrevRandao(),
        0,
        withdrawals.map(BodyValidation::withdrawalsRoot).orElse(null),
        blockParam.getBlobGasUsed(),
        blockParam.getExcessBlobGas() == null
            ? null
            : BlobGas.fromHexString(blockParam.getExcessBlobGas()),
        parentBeaconRoot.orElse(null),
        requests.map(BodyValidation::requestsHash).orElse(null),
        blockAccessList.map(BodyValidation::balHash).orElse(null),
        blockParam.getSlotNumber(),
        HEADER_FUNCTIONS);
  }
}
