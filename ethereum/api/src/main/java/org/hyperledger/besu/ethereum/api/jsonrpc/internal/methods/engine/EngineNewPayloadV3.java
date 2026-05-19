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
import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcRequestException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.ExecutionPayloadV3;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter.JsonRpcParameterException;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.NewPayloadRequestParametersV2;
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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes32;

public sealed class EngineNewPayloadV3 extends EngineNewPayloadV2 permits EngineNewPayloadV4 {

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
  protected NewPayloadRequestParametersV2 readRequestParameters(
      final JsonRpcRequestContext requestContext) {
    final NewPayloadRequestParametersV1 requestParameters =
        super.readRequestParameters(requestContext);
    final Optional<List<String>> expectedBlobVersionedHashes;
    try {
      expectedBlobVersionedHashes = requestContext.getOptionalList(1, String.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid versioned hash parameters (index 1)",
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          e);
    }

    final Optional<String> parentBeaconBlockRootParameter;
    try {
      parentBeaconBlockRootParameter = requestContext.getOptionalParameter(2, String.class);
    } catch (JsonRpcParameterException e) {
      throw new InvalidJsonRpcRequestException(
          "Invalid parent beacon block root parameters (index 2)",
          RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
          e);
    }

    return new NewPayloadRequestParametersV2(
        requestParameters, expectedBlobVersionedHashes, parentBeaconBlockRootParameter);
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(
      final NewPayloadRequestParametersV1 requestParameters) {
    final ValidationResult<RpcErrorType> result = super.validateParameters(requestParameters);
    if (!result.isValid()) {
      return result;
    }
    final var payloadParameter = requestParameters.payloadParameter();
    final ExecutionPayloadV3 executionPayload = (ExecutionPayloadV3) payloadParameter;
    final NewPayloadRequestParametersV2 requestParametersV2 =
        (NewPayloadRequestParametersV2) requestParameters;
    final Long blobGasUsed = executionPayload.getBlobGasUsed();
    final String excessBlobGas = executionPayload.getExcessBlobGas();
    final Optional<List<String>> expectedBlobVersionedHashes =
        requestParametersV2.expectedBlobVersionedHashes();
    final Optional<String> parentBeaconBlockRootParameter =
        requestParametersV2.parentBeaconBlockRootParameter();
    if (blobGasUsed == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_BLOB_GAS_USED_PARAMS, "Missing blob gas used field");
    } else if (excessBlobGas == null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS, "Missing excess blob gas field");
    } else if (expectedBlobVersionedHashes.isEmpty()) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS, "Missing versioned hashes field");
    } else if (parentBeaconBlockRootParameter.isEmpty()) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
          "Missing parent beacon block root field");
    } else {
      return ValidationResult.valid();
    }
  }

  @Override
  protected VersionSpecificPayloadData createVersionSpecificPayloadData(
      final NewPayloadRequestParametersV1 requestParameters)
      throws InvalidVersionSpecificPayloadException {
    final V2PayloadData v2PayloadData =
        (V2PayloadData) super.createVersionSpecificPayloadData(requestParameters);
    final NewPayloadRequestParametersV2 requestParametersV2 =
        (NewPayloadRequestParametersV2) requestParameters;
    try {
      return new V3PayloadData(
          v2PayloadData, parseVersionedHashes(requestParametersV2.expectedBlobVersionedHashes()));
    } catch (final RuntimeException e) {
      throw InvalidVersionSpecificPayloadException.invalidPayload("Invalid versionedHash");
    }
  }

  @Override
  protected void setVersionSpecificBlockHeaderFields(
      final BlockHeaderBuilder blockHeaderBuilder,
      final NewPayloadRequestParametersV1 requestParameters,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.setVersionSpecificBlockHeaderFields(
        blockHeaderBuilder, requestParameters, versionSpecificPayloadData);
    final ExecutionPayloadV3 payloadParameter =
        (ExecutionPayloadV3) requestParameters.payloadParameter();
    final NewPayloadRequestParametersV2 requestParametersV2 =
        (NewPayloadRequestParametersV2) requestParameters;
    blockHeaderBuilder
        .blobGasUsed(payloadParameter.getBlobGasUsed())
        .excessBlobGas(BlobGas.fromHexString(payloadParameter.getExcessBlobGas()))
        .parentBeaconBlockRoot(requestParametersV2.parentBeaconBlockRoot().orElse(null));
  }

  @Override
  protected ValidationResult<RpcErrorType> validateVersionSpecificBlockData(
      final List<Transaction> transactions,
      final BlockHeader header,
      final BlockHeader parentHeader,
      final ProtocolSpec protocolSpec,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    final ValidationResult<RpcErrorType> result =
        super.validateVersionSpecificBlockData(
            transactions, header, parentHeader, protocolSpec, versionSpecificPayloadData);
    if (!result.isValid()) {
      return result;
    }
    final V3PayloadData v3PayloadData = (V3PayloadData) versionSpecificPayloadData;
    final List<Transaction> blobTransactions =
        transactions.stream().filter(transaction -> transaction.getType().supportsBlob()).toList();
    return validateBlobTransactions(
        blobTransactions, header, parentHeader, v3PayloadData.maybeVersionedHashes(), protocolSpec);
  }

  protected ValidationResult<RpcErrorType> validateBlobs(
      final List<Transaction> blobTransactions,
      final BlockHeader header,
      final BlockHeader parentHeader,
      final Optional<List<VersionedHash>> maybeVersionedHashes,
      final ProtocolSpec protocolSpec) {
    return validateBlobTransactions(
        blobTransactions, header, parentHeader, maybeVersionedHashes, protocolSpec);
  }

  protected ValidationResult<RpcErrorType> validateBlobTransactions(
      final List<Transaction> blobTransactions,
      final BlockHeader header,
      final BlockHeader parentHeader,
      final Optional<List<VersionedHash>> maybeVersionedHashes,
      final ProtocolSpec protocolSpec) {

    final List<VersionedHash> transactionVersionedHashes = new ArrayList<>();
    long transactionBlobGasLimitCap =
        protocolSpec.getGasLimitCalculator().transactionBlobGasLimitCap();
    long blockBlobGasLimit = protocolSpec.getGasLimitCalculator().currentBlobGasLimit();
    for (Transaction transaction : blobTransactions) {
      var versionedHashes = transaction.getVersionedHashes();
      // blob transactions must have at least one blob
      if (versionedHashes.isEmpty()) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT, "There must be at least one blob");
      }
      int totalBlobCount = versionedHashes.get().size();
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
      transactionVersionedHashes.addAll(versionedHashes.get());
    }

    if (maybeVersionedHashes.isEmpty() && !transactionVersionedHashes.isEmpty()) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          "Payload must contain versioned hashes for transactions");
    }

    // Validate versionedHashesParam
    if (maybeVersionedHashes.isPresent()
        && !maybeVersionedHashes.get().equals(transactionVersionedHashes)) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          "Versioned hashes from blob transactions do not match expected values");
    }

    // Validate excessBlobGas
    Optional<BlobGas> maybeCalculatedExcess =
        validateExcessBlobGas(header, parentHeader, protocolSpec);
    if (maybeCalculatedExcess.isPresent()) {
      BlobGas calculated = maybeCalculatedExcess.get();
      BlobGas actual = header.getExcessBlobGas().orElse(BlobGas.ZERO);
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS,
          String.format(
              "Payload excessBlobGas does not match calculated excessBlobGas. Expected %s, got %s",
              calculated, actual));
    }

    // Validate blobGasUsed
    if (header.getBlobGasUsed().isPresent() && maybeVersionedHashes.isPresent()) {
      Optional<Long> maybeCalculatedBlobGas =
          validateBlobGasUsed(header, maybeVersionedHashes.get(), protocolSpec);
      if (maybeCalculatedBlobGas.isPresent()) {
        long calculated = maybeCalculatedBlobGas.get();
        long actual = header.getBlobGasUsed().orElse(0L);
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
    BlobGas calculated =
        ExcessBlobGasCalculator.calculateExcessBlobGasForParent(protocolSpec, parentHeader);
    BlobGas actual = header.getExcessBlobGas().orElse(BlobGas.ZERO);

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
    long calculated = protocolSpec.getGasCalculator().blobGasCost(versionedHashes.size());
    long actual = header.getBlobGasUsed().orElse(0L);

    return calculated == actual ? Optional.empty() : Optional.of(calculated);
  }

  protected Optional<List<VersionedHash>> parseVersionedHashes(
      final Optional<List<String>> maybeVersionedHashParam) {
    return maybeVersionedHashParam.map(
        versionedHashes ->
            versionedHashes.stream()
                .map(Bytes32::fromHexString)
                .map(
                    hash -> {
                      try {
                        return new VersionedHash(hash);
                      } catch (InvalidParameterException e) {
                        throw new RuntimeException(e);
                      }
                    })
                .toList());
  }

  @Override
  protected void appendVersionSpecificLogInfo(
      final StringBuilder message,
      final List<Object> messageArgs,
      final Block block,
      final List<Transaction> transactions,
      final VersionSpecificPayloadData versionSpecificPayloadData) {
    super.appendVersionSpecificLogInfo(
        message, messageArgs, block, transactions, versionSpecificPayloadData);
    final int blobCount =
        transactions.stream()
            .map(Transaction::getVersionedHashes)
            .flatMap(Optional::stream)
            .mapToInt(List::size)
            .sum();
    message.append("| %2d blobs");
    messageArgs.add(blobCount);
  }

  protected static class V3PayloadData extends V2PayloadData {
    private final Optional<List<VersionedHash>> maybeVersionedHashes;

    protected V3PayloadData(
        final V2PayloadData v2PayloadData,
        final Optional<List<VersionedHash>> maybeVersionedHashes) {
      super(v2PayloadData.maybeWithdrawals());
      this.maybeVersionedHashes = maybeVersionedHashes;
    }

    protected Optional<List<VersionedHash>> maybeVersionedHashes() {
      return maybeVersionedHashes;
    }
  }
}
