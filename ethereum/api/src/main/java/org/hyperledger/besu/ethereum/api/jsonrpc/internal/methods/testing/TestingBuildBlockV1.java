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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobsBundleV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.EngineGetPayloadResultV6;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.Quantity;
import org.hyperledger.besu.ethereum.api.util.DomainObjectDecodeUtils;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Request;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.util.HexUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The testing_buildBlockV1 RPC method is a debugging and testing tool that simplifies the block
 * production process into a single call. It is intended to replace the multi-step workflow of
 * sending transactions, calling engine_forkchoiceUpdated with payloadAttributes, and then calling
 * engine_getPayload.
 *
 * <p>This method is considered sensitive and is intended for testing environments only.
 */
public class TestingBuildBlockV1 implements JsonRpcMethod {

  private static final Logger LOG = LoggerFactory.getLogger(TestingBuildBlockV1.class);

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final MiningConfiguration miningConfiguration;
  private final TransactionPool transactionPool;
  private final EthScheduler ethScheduler;

  public TestingBuildBlockV1(
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final MiningConfiguration miningConfiguration,
      final TransactionPool transactionPool,
      final EthScheduler ethScheduler) {
    this.protocolContext = protocolContext;
    this.protocolSchedule = protocolSchedule;
    this.miningConfiguration = miningConfiguration;
    this.transactionPool = transactionPool;
    this.ethScheduler = ethScheduler;
  }

  @Override
  public String getName() {
    return RpcMethod.TESTING_BUILD_BLOCK_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Object requestId = requestContext.getRequest().getId();

    // Parameter 0: parentBlockHash (required)
    final Hash parentBlockHash;
    try {
      final String parentHashHex = requestContext.getRequiredParameter(0, String.class);
      parentBlockHash = Hash.fromHexString(parentHashHex);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid parentBlockHash parameter (index 0)", RpcErrorType.INVALID_PARAMS, e);
    }

    // Parameter 1: payloadAttributes (required)
    final EnginePayloadAttributesParameter payloadAttributes;
    try {
      payloadAttributes =
          requestContext.getRequiredParameter(1, EnginePayloadAttributesParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid payloadAttributes parameter (index 1)", RpcErrorType.INVALID_PARAMS, e);
    }

    // Parameter 2: transactions (can be null or array)
    // - null -> use txpool
    // - [] or [...] -> use provided transactions
    final String[] txArray;
    try {
      txArray = requestContext.getOptionalParameter(2, String[].class).orElse(null);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transactions parameter (index 2)", RpcErrorType.INVALID_PARAMS, e);
    }
    final List<String> rawTransactions = txArray != null ? List.of(txArray) : List.of();
    final boolean transactionsProvided = txArray != null;

    // Parameter 3: extraData (optional)
    final Bytes extraData;
    try {
      extraData =
          requestContext
              .getOptionalParameter(3, String.class)
              .filter(s -> !s.isEmpty())
              .map(Bytes::fromHexString)
              .orElse(Bytes.EMPTY);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid extraData parameter (index 3)", RpcErrorType.INVALID_PARAMS, e);
    }

    final Blockchain blockchain = protocolContext.getBlockchain();
    final Optional<BlockHeader> maybeParentHeader = blockchain.getBlockHeader(parentBlockHash);

    if (maybeParentHeader.isEmpty()) {
      return new JsonRpcErrorResponse(
          requestId,
          ValidationResult.invalid(
              RpcErrorType.INVALID_PARAMS, "Parent block not found: " + parentBlockHash));
    }

    final BlockHeader parentHeader = maybeParentHeader.get();

    if (payloadAttributes == null) {
      return new JsonRpcErrorResponse(
          requestId,
          ValidationResult.invalid(RpcErrorType.INVALID_PARAMS, "Missing payloadAttributes field"));
    }

    final ValidationResult<RpcErrorType> attributesValidation =
        validatePayloadAttributes(payloadAttributes);
    if (!attributesValidation.isValid()) {
      return new JsonRpcErrorResponse(requestId, attributesValidation);
    }

    final List<Transaction> transactions = new ArrayList<>();
    for (String rawTx : rawTransactions) {
      try {
        transactions.add(DomainObjectDecodeUtils.decodeRawTransaction(rawTx));
      } catch (Exception e) {
        LOG.debug("Failed to decode transaction: {}", rawTx, e);
        return new JsonRpcErrorResponse(
            requestId,
            ValidationResult.invalid(
                RpcErrorType.INVALID_TRANSACTION_PARAMS,
                "Failed to decode transaction: " + e.getMessage()));
      }
    }

    // Determine how to handle transactions based on go-ethereum semantics:
    // - If transactions field was not provided (null in JSON) -> use txpool (Optional.empty())
    // - If transactions field is empty array [] -> build empty block (Optional.of(emptyList))
    // - If transactions field has items -> use those (Optional.of(transactions))
    final boolean useTransactionsFromTxPool = !transactionsProvided;
    final Optional<List<Transaction>> maybeTransactions =
        useTransactionsFromTxPool ? Optional.empty() : Optional.of(transactions);

    final List<Withdrawal> withdrawals =
        payloadAttributes.getWithdrawals() != null
            ? payloadAttributes.getWithdrawals().stream()
                .map(WithdrawalParameter::toWithdrawal)
                .collect(Collectors.toList())
            : List.of();

    final Bytes32 prevRandao = payloadAttributes.getPrevRandao();
    final Bytes32 parentBeaconBlockRoot = payloadAttributes.getParentBeaconBlockRoot();
    final Long timestamp = payloadAttributes.getTimestamp();
    final Long slotNumber = payloadAttributes.getSlotNumber();

    try {
      final Address coinbase = payloadAttributes.getSuggestedFeeRecipient();

      final TestingBlockCreator blockCreator =
          new TestingBlockCreator(
              miningConfiguration,
              coinbase,
              extraData,
              transactionPool,
              protocolContext,
              protocolSchedule,
              ethScheduler);

      final BlockCreationResult result =
          blockCreator.createBlock(
              maybeTransactions,
              prevRandao,
              timestamp,
              Optional.of(withdrawals),
              Optional.ofNullable(parentBeaconBlockRoot),
              Optional.ofNullable(slotNumber),
              parentHeader);

      final Block block = result.getBlock();

      final List<String> txsAsHex =
          block.getBody().getTransactions().stream()
              .map(tx -> TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY))
              .map(b -> HexUtils.toFastHex(b, true))
              .collect(Collectors.toList());

      final Optional<List<String>> executionRequests = getExecutionRequests(result);

      final BlobsBundleV2 blobsBundle = new BlobsBundleV2(block.getBody().getTransactions());

      final String blockAccessListHex = encodeBlockAccessList(result.getBlockAccessList());

      final String slotNumberHex =
          block.getHeader().getOptionalSlotNumber().map(Quantity::create).orElse(null);

      final EngineGetPayloadResultV6 responsePayload =
          new EngineGetPayloadResultV6(
              block.getHeader(),
              txsAsHex,
              block.getBody().getWithdrawals(),
              executionRequests,
              Quantity.create(Wei.ZERO),
              blobsBundle,
              blockAccessListHex,
              slotNumberHex);

      return new JsonRpcSuccessResponse(requestId, responsePayload);

    } catch (Exception e) {
      LOG.error("Error building block", e);
      return new JsonRpcErrorResponse(
          requestId,
          ValidationResult.invalid(
              RpcErrorType.INTERNAL_ERROR, "Error building block: " + e.getMessage()));
    }
  }

  private ValidationResult<RpcErrorType> validatePayloadAttributes(
      final EnginePayloadAttributesParameter attributes) {
    if (attributes.getTimestamp() == null || attributes.getTimestamp() == 0) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_PARAMS, "Missing or invalid timestamp field");
    }
    if (attributes.getPrevRandao() == null) {
      return ValidationResult.invalid(RpcErrorType.INVALID_PARAMS, "Missing prevRandao field");
    }
    if (attributes.getSuggestedFeeRecipient() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_PARAMS, "Missing suggestedFeeRecipient field");
    }
    return ValidationResult.valid();
  }

  private Optional<List<String>> getExecutionRequests(final BlockCreationResult result) {
    return result
        .getRequests()
        .map(
            requests ->
                requests.stream()
                    .sorted(Comparator.comparing(Request::getType))
                    .filter(r -> !r.getData().isEmpty())
                    .map(Request::getEncodedRequest)
                    .map(b -> HexUtils.toFastHex(b, true))
                    .toList());
  }

  private String encodeBlockAccessList(final Optional<BlockAccessList> maybeBlockAccessList) {
    return maybeBlockAccessList
        .map(
            bal -> {
              final BytesValueRLPOutput output = new BytesValueRLPOutput();
              bal.writeTo(output);
              return output.encoded().toHexString();
            })
        .orElse(null);
  }
}
