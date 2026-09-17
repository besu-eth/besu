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

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobCellsAndProofsV1;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import jakarta.validation.constraints.NotNull;
import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of engine_getBlobsV4 API method.
 *
 * <p>Unlike {@code engine_getBlobsV3}, this method returns only the individual cells (and their KZG
 * proofs) selected by a caller-supplied indices bitarray, rather than full blobs.
 *
 * <p>Specification:
 *
 * <ul>
 *   <li>Returns partial responses with null entries for missing blobs
 *   <li>Supports at least 128 blob versioned hashes per request
 *   <li>Only supports KZG_CELL_PROOFS blob type (rejects KZG_PROOF)
 *   <li>Each returned {@link BlobCellsAndProofsV1} contains only the cells/proofs at the indices
 *       set in {@code indices_bitarray}
 * </ul>
 */
public class EngineGetBlobsV4 extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV4.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;
  private static final int INDICES_BITARRAY_BYTE_LENGTH = 16;

  private final TransactionPool transactionPool;
  protected final GetBlobsMetrics getBlobsMetrics;

  public EngineGetBlobsV4(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
    this.transactionPool = constructorArguments.transactionPool();
    this.getBlobsMetrics =
        new GetBlobsMetrics(constructorArguments.metricsSystem(), getNumericVersion());
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_BLOBS_V4.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    final VersionedHash[] versionedHashes = extractVersionedHashes(requestContext);
    final Bytes indicesBitarray = extractIndicesBitarray(requestContext);
    if (versionedHashes.length > REQUEST_MAX_VERSIONED_HASHES) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST);
    }
    if (mergeContext.get().isSyncing()) {
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }
    long timestamp = protocolContext.getBlockchain().getChainHeadHeader().getTimestamp();
    ValidationResult<RpcErrorType> forkValidationResult = validateForkSupported(timestamp);
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), forkValidationResult);
    }

    getBlobsMetrics.increaseRequested(versionedHashes.length);

    final List<Integer> cellIndexes = cellIndexesFor(indicesBitarray);
    final List<BlobCellsAndProofsV1> result = getBlobV4Result(versionedHashes, cellIndexes);

    // count available blobs (non-null entries)
    final int availableCount = (int) result.stream().filter(Objects::nonNull).count();
    getBlobsMetrics.increaseAvailable(availableCount);
    getBlobsMetrics.increaseMissing(versionedHashes.length - availableCount);

    // track if this was a partial or full response
    if (availableCount == versionedHashes.length) {
      getBlobsMetrics.increaseFull();
    } else {
      getBlobsMetrics.increasePartial();
    }

    LOG.atDebug()
        .setMessage("Requested {} bundles, found {} valid bundles, {} missing")
        .addArgument(versionedHashes.length)
        .addArgument(availableCount)
        .addArgument(() -> versionedHashes.length - availableCount)
        .log();

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }

  private VersionedHash[] extractVersionedHashes(final JsonRpcRequestContext requestContext) {
    try {
      return requestContext.getRequiredParameter(0, VersionedHash[].class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid versioned hashes parameter (index 0)",
          RpcErrorType.INVALID_VERSIONED_HASHES_PARAMS,
          e);
    }
  }

  private Bytes extractIndicesBitarray(final JsonRpcRequestContext requestContext) {
    final Bytes indicesBitarray;
    try {
      indicesBitarray = requestContext.getRequiredParameter(1, Bytes.class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid indices bitarray parameter (index 1)",
          RpcErrorType.INVALID_INDICES_BITARRAY_PARAMS,
          e);
    }
    if (indicesBitarray.size() != INDICES_BITARRAY_BYTE_LENGTH) {
      throw new InvalidJsonRpcParameters(
          "Invalid indices bitarray parameter (index 1): expected %d bytes, got %d"
              .formatted(INDICES_BITARRAY_BYTE_LENGTH, indicesBitarray.size()),
          RpcErrorType.INVALID_INDICES_BITARRAY_PARAMS);
    }
    return indicesBitarray;
  }

  private List<Integer> cellIndexesFor(final Bytes indicesBitarray) {
    final List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < CKZG4844Helper.CELL_PROOFS_PER_BLOB; i++) {
      final int byteIndex = i / Byte.SIZE;
      final int bitIndex = i % Byte.SIZE;
      if ((Byte.toUnsignedInt(indicesBitarray.get(byteIndex)) & (1 << bitIndex)) != 0) {
        indexes.add(i);
      }
    }
    return indexes;
  }

  private @NotNull List<BlobCellsAndProofsV1> getBlobV4Result(
      final VersionedHash[] versionedHashes, final List<Integer> cellIndexes) {
    return Arrays.stream(versionedHashes)
        .map(transactionPool::getBlobProofBundle)
        .map(bundle -> getBlobCellsAndProofsV1(bundle, cellIndexes))
        .toList();
  }

  private @Nullable BlobCellsAndProofsV1 getBlobCellsAndProofsV1(
      final BlobProofBundle bundle, final List<Integer> cellIndexes) {
    if (bundle == null) {
      return null;
    }
    // Only KZG_CELL_PROOFS blobs support cell-level extraction, reject KZG_PROOF
    if (bundle.getBlobType() == BlobType.KZG_PROOF) {
      LOG.debug(
          "Unsupported blob type KZG_PROOF for versioned hash: {}", bundle.getVersionedHash());
      return null;
    }
    final Bytes blobCells = bundle.getBlobCellsBytes().orElse(null);
    if (blobCells == null) {
      return null;
    }
    final int cellSize = blobCells.size() / CKZG4844Helper.CELL_PROOFS_PER_BLOB;
    final List<Bytes> cells =
        cellIndexes.stream().map(index -> blobCells.slice(index * cellSize, cellSize)).toList();
    final List<KZGProof> proofs =
        cellIndexes.stream().map(index -> bundle.getKzgProof().get(index)).toList();
    return new BlobCellsAndProofsV1(cells, proofs);
  }
}
