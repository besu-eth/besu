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

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.Configuration.FAIL_ON_UNKNOWN_BUT_NULL;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV2;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderBuilder;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;

public sealed class EngineNewPayloadV3<
        EP extends ExecutionPayloadV3, NPRP extends NewPayloadRequestParametersV2<? extends EP>>
    extends EngineNewPayloadV2<EP, NPRP> permits EngineNewPayloadV4 {

  public EngineNewPayloadV3(
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
    return RpcMethod.ENGINE_NEW_PAYLOAD_V3.getMethodName();
  }

  @Override
  protected Class<? extends ExecutionPayloadV1> getPayloadParameterClass() {
    return ExecutionPayloadV3.class;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected NPRP readRequestParameters(final JsonRpcRequestContext requestContext) {
    final NewPayloadRequestParametersV1<? extends EP> requestParameters =
        super.readRequestParameters(requestContext);

    final List<VersionedHash> expectedBlobVersionedHashes;
    try {
      expectedBlobVersionedHashes =
          requestContext.getRequiredList(1, VersionedHash.class, FAIL_ON_UNKNOWN_BUT_NULL);
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          requestParameters.payloadParameter(),
          "Invalid versioned hash parameters (index 1)",
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          e);
    }

    final Bytes32 parentBeaconBlockRootParameter;
    try {
      parentBeaconBlockRootParameter =
          requestContext.getRequiredParameter(2, Bytes32.class, FAIL_ON_UNKNOWN_BUT_NULL);
    } catch (JsonRpcParameterException e) {
      throw new InvalidRequestParametersException(
          requestParameters.payloadParameter(),
          "Invalid parent beacon block root parameters (index 2)",
          RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
          e);
    }

    return (NPRP)
        new NewPayloadRequestParametersV2<>(
            requestParameters, expectedBlobVersionedHashes, parentBeaconBlockRootParameter);
  }

  @Override
  protected int getNumberOfParameters() {
    return 3;
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(final NPRP requestParameters) {
    final ValidationResult<RpcErrorType> result = super.validateParameters(requestParameters);
    return result.isValid() ? validateParametersV3(requestParameters) : result;
  }

  private ValidationResult<RpcErrorType> validateParametersV3(
      final NewPayloadRequestParametersV2<? extends EP> requestParameters) {
    final ExecutionPayloadV3 executionPayload = requestParameters.payloadParameter();
    if (executionPayload.getBlobGasUsed() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_BLOB_GAS_USED_PARAMS, "Missing blob gas used field");
    } else if (executionPayload.getExcessBlobGas() == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS, "Missing excess blob gas field");
    }
    return ValidationResult.valid();
  }

  @Override
  protected void setBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder, final NPRP requestParameters) {
    super.setBlockHeaderFields(blockHeaderBuilder, requestParameters);
    final ExecutionPayloadV3 payloadParameter = requestParameters.payloadParameter();
    blockHeaderBuilder
        .blobGasUsed(payloadParameter.getBlobGasUsed())
        .excessBlobGas(payloadParameter.getExcessBlobGas())
        .parentBeaconBlockRoot(requestParameters.parentBeaconBlockRoot());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateNewBlock(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final Optional<BlockHeader> maybeParentHeader,
      final NPRP requestParameters) {
    final ValidationResult<RpcErrorType> result =
        super.validateNewBlock(newBlock, protocolSpec, maybeParentHeader, requestParameters);
    return result.isValid()
        ? validateExecutionPayloadV3(newBlock, protocolSpec, maybeParentHeader, requestParameters)
        : result;
  }

  private ValidationResult<RpcErrorType> validateExecutionPayloadV3(
      final Block newBlock,
      final ProtocolSpec protocolSpec,
      final Optional<BlockHeader> maybeParentHeader,
      final NewPayloadRequestParametersV2<? extends EP> requestParameters) {
    final List<Transaction> blobTransactions =
        newBlock.getBody().getTransactions().stream()
            .filter(transaction -> transaction.getType().supportsBlob())
            .toList();
    return validateBlobTransactions(
        blobTransactions,
        newBlock.getHeader(),
        maybeParentHeader,
        requestParameters.expectedBlobVersionedHashes(),
        protocolSpec);
  }

  protected ValidationResult<RpcErrorType> validateBlobTransactions(
      final List<Transaction> blobTransactions,
      final BlockHeader header,
      final Optional<BlockHeader> maybeParentHeader,
      final List<VersionedHash> versionedHashesParam,
      final ProtocolSpec protocolSpec) {

    final List<VersionedHash> transactionVersionedHashes =
        new ArrayList<>(versionedHashesParam.size());
    final long transactionBlobGasLimitCap =
        protocolSpec.getGasLimitCalculator().transactionBlobGasLimitCap();
    final long blockBlobGasLimit = protocolSpec.getGasLimitCalculator().currentBlobGasLimit();
    for (Transaction transaction : blobTransactions) {
      final var maybeTxVersionedHashes = transaction.getVersionedHashes();
      // blob transactions must have at least one blob
      if (maybeTxVersionedHashes.isEmpty()) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT, "There must be at least one blob");
      }
      final List<VersionedHash> txVersionedHashes = maybeTxVersionedHashes.get();
      final int totalBlobCount = txVersionedHashes.size();
      long transactionBlobGasUsed = protocolSpec.getGasCalculator().blobGasCost(totalBlobCount);
      // Check if blob gas used by tx exceeds block blob gas limit
      if (transactionBlobGasUsed > blockBlobGasLimit) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT,
            String.format(
                "Blob transaction %s exceeds block blob gas limit: %d > %d",
                transaction.getHash(), transactionBlobGasUsed, blockBlobGasLimit));
      }
      // Check if blob gas used by tx exceeds transaction cap
      if (transactionBlobGasUsed > transactionBlobGasLimitCap) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT,
            String.format("Blob transaction has too many blobs: %d", totalBlobCount));
      }
      transactionVersionedHashes.addAll(txVersionedHashes);
    }

    // Validate versionedHashesParam
    if (!versionedHashesParam.equals(transactionVersionedHashes)) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          "Versioned hashes from blob transactions do not match expected values");
    }

    // Validate excessBlobGas
    if (maybeParentHeader.isPresent()) {
      final Optional<BlobGas> maybeCalculatedExcess =
          validateExcessBlobGas(header, maybeParentHeader.get(), protocolSpec);
      if (maybeCalculatedExcess.isPresent()) {
        final BlobGas calculated = maybeCalculatedExcess.get();
        final BlobGas actual = header.getExcessBlobGas().orElse(BlobGas.ZERO);
        return ValidationResult.invalid(
            RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS,
            String.format(
                "Payload excessBlobGas does not match calculated excessBlobGas. Expected %s, got %s",
                calculated, actual));
      }
    }

    // Validate blobGasUsed
    if (header.getBlobGasUsed().isPresent()) {
      final Optional<Long> maybeCalculatedBlobGas =
          validateBlobGasUsed(header, versionedHashesParam, protocolSpec);
      if (maybeCalculatedBlobGas.isPresent()) {
        final long calculated = maybeCalculatedBlobGas.get();
        final long actual = header.getBlobGasUsed().orElse(0L);
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_GAS_USED_PARAMS,
            String.format(
                "Payload BlobGasUsed does not match calculated BlobGasUsed. Expected %s, got %s",
                calculated, actual));
      }
    }

    if (protocolSpec.getGasCalculator().blobGasCost(transactionVersionedHashes.size())
        > protocolSpec.getGasLimitCalculator().currentBlobGasLimit()) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_BLOB_COUNT,
          String.format("Invalid Blob Count: %d", transactionVersionedHashes.size()));
    }
    return ValidationResult.valid();
  }

  /**
   * Validates that the excessBlobGas in the header matches the calculated value from the parent
   * header. Returns Optional.of(calculated) if mismatched, otherwise Optional.empty().
   */
  @VisibleForTesting
  Optional<BlobGas> validateExcessBlobGas(
      final BlockHeader header, final BlockHeader parentHeader, final ProtocolSpec protocolSpec) {
    final BlobGas calculated =
        ExcessBlobGasCalculator.calculateExcessBlobGasForParent(protocolSpec, parentHeader);
    final BlobGas actual = header.getExcessBlobGas().orElse(BlobGas.ZERO);

    return calculated.equals(actual) ? Optional.empty() : Optional.of(calculated);
  }

  /**
   * Validates that blobGasUsed in the header matches the calculated value from the versioned
   * hashes. Returns Optional.of(calculated) if mismatched, otherwise Optional.empty().
   */
  @VisibleForTesting
  Optional<Long> validateBlobGasUsed(
      final BlockHeader header,
      final List<VersionedHash> versionedHashes,
      final ProtocolSpec protocolSpec) {
    final long calculated = protocolSpec.getGasCalculator().blobGasCost(versionedHashes.size());
    final long actual = header.getBlobGasUsed().orElse(0L);

    return calculated == actual ? Optional.empty() : Optional.of(calculated);
  }

  @Override
  protected void appendVersionSpecificLogInfo(
      final StringBuilder message, final List<Object> messageArgs, final Block block) {
    super.appendVersionSpecificLogInfo(message, messageArgs, block);
    final int blobCount =
        block.getBody().getTransactions().stream()
            .map(Transaction::getVersionedHashes)
            .flatMap(Optional::stream)
            .mapToInt(List::size)
            .sum();
    message.append("| %2d blobs");
    messageArgs.add(blobCount);
  }

  @Override
  protected JsonRpcResponse processParametersParsingException(
      final Object reqId, final InvalidRequestParametersException e) {

    if (e.getRpcErrorType() == RpcErrorType.INVALID_VERSIONED_HASH_PARAMS) {
      // here we need to distinguish between null and invalid parameters and return different
      // responses
      final Optional<JsonRpcParameter.JsonRpcMissingParameterException> maybeMissingEx =
          extractCauseByType(e, JsonRpcParameter.JsonRpcMissingParameterException.class);
      if (maybeMissingEx.isPresent()) {
        return new JsonRpcErrorResponse(
            reqId,
            ValidationResult.invalid(
                RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
                "Missing versioned hashes parameter (" + maybeMissingEx.get().getMessage() + ")"));
      }
      return respondWithInvalid(
          reqId, null, null, INVALID, RpcErrorType.INVALID_VERSIONED_HASH_PARAMS.getMessage());
    }

    if (e.getRpcErrorType() == RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS) {
      return new JsonRpcErrorResponse(
          reqId,
          ValidationResult.invalid(
              RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
              "Failed to decode parent beacon block root parameter (" + e.getMessage() + ")"));
    }
    return super.processParametersParsingException(reqId, e);
  }
}
