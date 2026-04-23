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

import org.hyperledger.besu.datatypes.BlobGas;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.ethereum.mainnet.feemarket.ExcessBlobGasCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

/**
 * Stateless spec-driven validations for {@code engine_newPayload}: per-field presence and blob /
 * blob-gas / excess-blob-gas consistency. The class holds no state and cannot be instantiated.
 */
public final class NewPayloadValidators {

  private NewPayloadValidators() {}

  public static ValidationResult<RpcErrorType> validateFieldPresence(
      final NewPayloadRules rules,
      final EnginePayloadParameter parameter,
      final Optional<List<String>> maybeVersionedHashParam,
      final Optional<String> maybeBeaconBlockRootParam,
      final Optional<List<String>> maybeRequestsParam) {
    ValidationResult<RpcErrorType> result;

    result =
        checkPresence(
            parameter.getBlobGasUsed() != null,
            rules.blobGasUsed(),
            RpcErrorType.INVALID_BLOB_GAS_USED_PARAMS,
            "blob gas used");
    if (!result.isValid()) return result;

    result =
        checkPresence(
            parameter.getExcessBlobGas() != null,
            rules.excessBlobGas(),
            RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS,
            "excess blob gas");
    if (!result.isValid()) return result;

    result =
        checkPresence(
            maybeVersionedHashParam != null && maybeVersionedHashParam.isPresent(),
            rules.versionedHashes(),
            RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
            "versioned hashes");
    if (!result.isValid()) return result;

    result =
        checkPresence(
            maybeBeaconBlockRootParam.isPresent(),
            rules.parentBeaconBlockRoot(),
            RpcErrorType.INVALID_PARENT_BEACON_BLOCK_ROOT_PARAMS,
            "parent beacon block root");
    if (!result.isValid()) return result;

    result =
        checkPresence(
            maybeRequestsParam.isPresent(),
            rules.executionRequests(),
            RpcErrorType.INVALID_EXECUTION_REQUESTS_PARAMS,
            "execution requests");
    if (!result.isValid()) return result;

    result =
        checkPresence(
            parameter.getSlotNumber() != null,
            rules.slotNumber(),
            RpcErrorType.INVALID_SLOT_NUMBER_PARAMS,
            "slot number");
    if (!result.isValid()) return result;

    return ValidationResult.valid();
  }

  static ValidationResult<RpcErrorType> checkPresence(
      final boolean isPresent,
      final FieldPresence rule,
      final RpcErrorType errorType,
      final String fieldName) {
    switch (rule) {
      case REQUIRED:
        if (!isPresent) {
          return ValidationResult.invalid(errorType, "Missing " + fieldName + " field");
        }
        break;
      case FORBIDDEN:
        if (isPresent) {
          return ValidationResult.invalid(errorType, "Unexpected " + fieldName + " field present");
        }
        break;
      case IGNORED:
        break;
    }
    return ValidationResult.valid();
  }

  public static ValidationResult<RpcErrorType> validateBlobs(
      final NewPayloadRules rules,
      final List<Transaction> blobTransactions,
      final BlockHeader header,
      final Optional<BlockHeader> maybeParentHeader,
      final Optional<List<VersionedHash>> maybeVersionedHashes,
      final ProtocolSpec protocolSpec) {

    if (rules.blobValidation() == BlobValidationMode.BYPASSED) {
      return ValidationResult.valid();
    }

    final List<VersionedHash> transactionVersionedHashes = new ArrayList<>();
    final long transactionBlobGasLimitCap =
        protocolSpec.getGasLimitCalculator().transactionBlobGasLimitCap();
    final long blockBlobGasLimit = protocolSpec.getGasLimitCalculator().currentBlobGasLimit();
    for (final Transaction transaction : blobTransactions) {
      final var versionedHashes = transaction.getVersionedHashes();
      if (versionedHashes.isEmpty()) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT, "There must be at least one blob");
      }
      final int totalBlobCount = versionedHashes.get().size();
      final long transactionBlobGasUsed =
          protocolSpec.getGasCalculator().blobGasCost(totalBlobCount);
      if (transactionBlobGasUsed > blockBlobGasLimit) {
        return ValidationResult.invalid(
            RpcErrorType.INVALID_BLOB_COUNT,
            String.format(
                "Blob transaction %s exceeds block blob gas limit: %d > %d",
                transaction.getHash(), transactionBlobGasUsed, blockBlobGasLimit));
      }
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

    if (maybeVersionedHashes.isPresent()
        && !maybeVersionedHashes.get().equals(transactionVersionedHashes)) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_VERSIONED_HASH_PARAMS,
          "Versioned hashes from blob transactions do not match expected values");
    }

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

    if (header.getBlobGasUsed().isPresent() && maybeVersionedHashes.isPresent()) {
      final Optional<Long> maybeCalculatedBlobGas =
          validateBlobGasUsed(header, maybeVersionedHashes.get(), protocolSpec);
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

  @VisibleForTesting
  static Optional<BlobGas> validateExcessBlobGas(
      final BlockHeader header, final BlockHeader parentHeader, final ProtocolSpec protocolSpec) {
    final BlobGas calculated =
        ExcessBlobGasCalculator.calculateExcessBlobGasForParent(protocolSpec, parentHeader);
    final BlobGas actual = header.getExcessBlobGas().orElse(BlobGas.ZERO);
    return calculated.equals(actual) ? Optional.empty() : Optional.of(calculated);
  }

  @VisibleForTesting
  static Optional<Long> validateBlobGasUsed(
      final BlockHeader header,
      final List<VersionedHash> versionedHashes,
      final ProtocolSpec protocolSpec) {
    final long calculated = protocolSpec.getGasCalculator().blobGasCost(versionedHashes.size());
    final long actual = header.getBlobGasUsed().orElse(0L);
    return calculated == actual ? Optional.empty() : Optional.of(calculated);
  }
}
