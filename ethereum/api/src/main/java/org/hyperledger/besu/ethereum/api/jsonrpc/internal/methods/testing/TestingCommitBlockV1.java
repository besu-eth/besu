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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.testing;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadAttributesParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.WithdrawalParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcError;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.util.DomainObjectDecodeUtils;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.Withdrawal;
import org.hyperledger.besu.ethereum.eth.manager.EthScheduler;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@code testing_commitBlockV1}: builds a block from the provided attributes on
 * top of the current canonical head, inserts it into the chain, and sets it as the new canonical
 * head.
 *
 * <p>This is the write companion of {@code testing_buildBlockV1}: it produces the same block but
 * additionally commits it, skipping the {@code engine_newPayload} + {@code
 * engine_forkchoiceUpdated} round-trip. Intended for testing environments only.
 */
public class TestingCommitBlockV1 implements JsonRpcMethod {

  private static final Logger LOG = LoggerFactory.getLogger(TestingCommitBlockV1.class);

  private final ProtocolContext protocolContext;
  private final ProtocolSchedule protocolSchedule;
  private final MiningConfiguration miningConfiguration;
  private final TransactionPool transactionPool;
  private final EthScheduler ethScheduler;

  public TestingCommitBlockV1(
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
    return RpcMethod.TESTING_COMMIT_BLOCK_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    final Object requestId = requestContext.getRequest().getId();

    // Parameter 0: payloadAttributes (required)
    final EnginePayloadAttributesParameter payloadAttributes;
    try {
      payloadAttributes =
          requestContext.getRequiredParameter(0, EnginePayloadAttributesParameter.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid payloadAttributes parameter (index 0)", RpcErrorType.INVALID_PARAMS, e);
    }

    // Parameter 1: transactions (required, but nullable)
    // - null  → build from mempool
    // - []    → build empty block
    // - [...] → use exactly these transactions
    final String[] txArray;
    try {
      txArray = requestContext.getOptionalParameter(1, String[].class).orElse(null);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid transactions parameter (index 1)", RpcErrorType.INVALID_PARAMS, e);
    }
    final boolean transactionsProvided = txArray != null;
    final List<String> rawTransactions = transactionsProvided ? List.of(txArray) : List.of();

    // Parameter 2: extraData (optional)
    final Bytes extraData;
    try {
      extraData =
          requestContext
              .getOptionalParameter(2, String.class)
              .filter(s -> !s.isEmpty())
              .map(Bytes::fromHexString)
              .orElse(Bytes.EMPTY);
    } catch (JsonRpcParameterException | IllegalArgumentException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid extraData parameter (index 2)", RpcErrorType.INVALID_PARAMS, e);
    }

    final ValidationResult<RpcErrorType> attributesValidation =
        validatePayloadAttributes(payloadAttributes);
    if (!attributesValidation.isValid()) {
      return new JsonRpcErrorResponse(requestId, attributesValidation);
    }

    final List<Transaction> transactions = new ArrayList<>();
    for (final String rawTx : rawTransactions) {
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

    final Optional<List<Transaction>> maybeTransactions =
        transactionsProvided ? Optional.of(transactions) : Optional.empty();

    final List<Withdrawal> withdrawals =
        payloadAttributes.getWithdrawals() != null
            ? payloadAttributes.getWithdrawals().stream()
                .map(WithdrawalParameter::toWithdrawal)
                .collect(Collectors.toList())
            : List.of();

    // Always build on top of the current canonical head — no parentHash parameter.
    final BlockHeader parentHeader = protocolContext.getBlockchain().getChainHeadHeader();

    try {
      final Address coinbase = payloadAttributes.getSuggestedFeeRecipient();
      miningConfiguration.setCoinbase(coinbase);

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
              payloadAttributes.getPrevRandao(),
              payloadAttributes.getTimestamp(),
              Optional.of(withdrawals),
              Optional.ofNullable(payloadAttributes.getParentBeaconBlockRoot()),
              Optional.ofNullable(payloadAttributes.getSlotNumber()),
              Optional.of(
                  Objects.requireNonNullElseGet(
                      payloadAttributes.getTargetGasLimit(),
                      () ->
                          protocolContext
                              .getBlockchain()
                              .getGenesisBlock()
                              .getHeader()
                              .getGasLimit())),
              parentHeader);

      if (transactionsProvided) {
        final Map<Transaction, TransactionSelectionResult> notSelected =
            result.getTransactionSelectionResults().getNotSelectedTransactions();
        for (final Transaction tx : transactions) {
          final TransactionSelectionResult selectionResult = notSelected.get(tx);
          if (selectionResult != null) {
            final String reason =
                selectionResult.maybeInvalidReason().orElse("transaction not applicable");
            return new JsonRpcErrorResponse(requestId, new JsonRpcError(-32000, reason, null));
          }
        }
      }

      final Block block = result.getBlock();
      final BlockImporter blockImporter =
          protocolSchedule.getByBlockHeader(block.getHeader()).getBlockImporter();

      // Header validation is NONE: we just built the block ourselves and know it is valid.
      // The importer still re-processes transactions to update the world state.
      final BlockImportResult importResult =
          blockImporter.importBlock(
              protocolContext,
              block,
              HeaderValidationMode.NONE,
              HeaderValidationMode.NONE,
              result.getBlockAccessList());

      if (!importResult.isImported()) {
        LOG.error("Failed to import block {} after building it", block.getHash());
        return new JsonRpcErrorResponse(
            requestId,
            ValidationResult.invalid(
                RpcErrorType.INTERNAL_ERROR, "Block was built but could not be imported"));
      }

      return new JsonRpcSuccessResponse(requestId, block.getHash().toHexString());

    } catch (Exception e) {
      LOG.error("Error building or committing block", e);
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
}
