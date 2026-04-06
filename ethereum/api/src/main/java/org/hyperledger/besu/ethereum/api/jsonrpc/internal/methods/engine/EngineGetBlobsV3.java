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

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;

import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.util.HexUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of engine_getBlobsV3 API method.
 *
 * <p>This method combines the partial response capability of V1 with the blob type support and
 * result format of V2. It returns an array matching the input order, with null entries for missing
 * or unsupported blobs, and supports KZG_CELL_PROOFS blob types introduced in Osaka.
 *
 * <p>Specification:
 *
 * <ul>
 *   <li>Returns partial responses with null entries for missing blobs
 *   <li>Supports at least 128 blob versioned hashes per request
 *   <li>Uses BlobAndProofV2 result format with cell proofs
 *   <li>Only supports KZG_CELL_PROOFS blob type (rejects KZG_PROOF)
 * </ul>
 */
public class EngineGetBlobsV3 extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV3.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;

  private final TransactionPool transactionPool;
  private final Counter requestedCounter;
  private final Counter availableCounter;
  private final Counter partialResponseCounter;
  private final Counter fullResponseCounter;
  private final Optional<Long> osakaMilestone;

  public EngineGetBlobsV3(
      final Vertx vertx,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final EngineCallListener engineCallListener,
      final TransactionPool transactionPool,
      final MetricsSystem metricsSystem) {
    super(vertx, protocolSchedule, protocolContext, engineCallListener);
    this.transactionPool = transactionPool;
    // create counters
    this.requestedCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_v3_requested_total",
            "Number of blobs requested via engine_getBlobsV3");
    this.availableCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_v3_available_total",
            "Number of blobs requested via engine_getBlobsV3 that are present in the blob pool");
    this.partialResponseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_v3_partial_total",
            "Number of calls to engine_getBlobsV3 that returned partial responses");
    this.fullResponseCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_v3_full_total",
            "Number of calls to engine_getBlobsV3 that returned complete responses");
    this.osakaMilestone = protocolSchedule.milestoneFor(OSAKA);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_BLOBS_V3.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    final VersionedHash[] versionedHashes = extractVersionedHashes(requestContext);
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

    requestedCounter.inc(versionedHashes.length);

    final List<BlobAndProofV2> result = getBlobV3Result(versionedHashes);

    // count available blobs (non-null entries)
    long availableCount = result.stream().mapToLong(blob -> blob != null ? 1 : 0).sum();
    availableCounter.inc(availableCount);

    // track if this was a partial or full response
    if (availableCount == versionedHashes.length) {
      fullResponseCounter.inc();
    } else {
      partialResponseCounter.inc();
    }

    LOG.debug(
        "Requested {} bundles, found {} valid bundles ({} partial response)",
        versionedHashes.length,
        availableCount,
        availableCount < versionedHashes.length);

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

  private @NotNull List<BlobAndProofV2> getBlobV3Result(final VersionedHash[] versionedHashes) {
    return Arrays.stream(versionedHashes)
        .map(transactionPool::getBlobProofBundle)
        .map(this::getBlobAndProofV2)
        .toList();
  }

  private @Nullable BlobAndProofV2 getBlobAndProofV2(final BlobProofBundle bundle) {
    if (bundle == null) {
      return null;
    }
    // V3 only supports KZG_CELL_PROOFS (like V2), reject KZG_PROOF
    if (bundle.getBlobType() == BlobType.KZG_PROOF) {
      LOG.trace(
          "Unsupported blob type KZG_PROOF for versioned hash: {}", bundle.getVersionedHash());
      return null;
    }
    return createBlobAndProofV2(bundle);
  }

  private BlobAndProofV2 createBlobAndProofV2(final BlobProofBundle blobProofBundle) {
    return new BlobAndProofV2(
        HexUtils.toFastHex(blobProofBundle.getBlob().getData(), true),
        blobProofBundle.getKzgProof().parallelStream()
            .map(proof -> HexUtils.toFastHex(proof.getData(), true))
            .toList());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateForkSupported(final long currentTimestamp) {
    return ForkSupportHelper.validateForkSupported(OSAKA, osakaMilestone, currentTimestamp);
  }
}
