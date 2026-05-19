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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineGetBlobsV2 extends ExecutionEngineJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV2.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;

  private final TransactionPool transactionPool;
  private final Counter requestedCounter;
  private final Counter availableCounter;
  private final Counter hitCounter;
  private final Counter missCounter;
  private final Optional<Long> osakaMilestone;

  public EngineGetBlobsV2(
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
            "execution_engine_getblobs_requested_total",
            "Number of blobs requested via engine_getBlobsV2");
    this.availableCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_available_total",
            "Number of blobs requested via engine_getBlobsV2 that are present in the blob pool");
    this.hitCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_hit_total",
            "Number of calls to engine_getBlobsV2 that returned at least one blob");
    this.missCounter =
        metricsSystem.createCounter(
            BesuMetricCategory.RPC,
            "execution_engine_getblobs_miss_total",
            "Number of calls to engine_getBlobsV2 that returned zero blobs");
    this.osakaMilestone = protocolSchedule.milestoneFor(OSAKA);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_BLOBS_V2.getMethodName();
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
    List<BlobProofBundle> validBundles = new ArrayList<>(versionedHashes.length);
    int missingBlobs = 0;
    int unsupportedBlobs = 0;
    for (VersionedHash hash : versionedHashes) {
      final BlobProofBundle bundle = transactionPool.getBlobProofBundle(hash);
      if (bundle == null) {
        LOG.trace("No BlobProofBundle found for versioned hash: {}", hash);
        missingBlobs++;
        continue;
      }
      if (bundle.getBlobType() == BlobType.KZG_PROOF) {
        LOG.trace("Unsupported blob type KZG_PROOF for versioned hash: {}", hash);
        unsupportedBlobs++;
        continue;
      }
      validBundles.add(bundle);
    }
    // count how many of the requested blobs are actually available
    availableCounter.inc(validBundles.size());

    LOG.debug(
        "Requested {} bundles, found {} valid bundles, {} missing, {} unsupported",
        versionedHashes.length,
        validBundles.size(),
        missingBlobs,
        unsupportedBlobs);

    // V2 returns null if any requested blobs are missing or unsupported
    if (missingBlobs > 0 || unsupportedBlobs > 0) {
      missCounter.inc();
      return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), null);
    }

    final List<BlobAndProofV2> results =
        validBundles.parallelStream().map(this::createBlobAndProofV2).toList();

    hitCounter.inc();
    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), results);
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

  private BlobAndProofV2 createBlobAndProofV2(final BlobProofBundle blobProofBundle) {
    return new BlobAndProofV2(
        HexUtils.toFastHex(blobProofBundle.getBlob().getData(), true),
        blobProofBundle.getKzgProof().parallelStream()
            .map(proof -> HexUtils.toFastHex(proof.getData(), true))
            .toList());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateForkSupported(final long currentTimestamp) {
    return osakaMilestone
        .map(
            milestone ->
                ForkSupportHelper.validateForkSupported(OSAKA, milestone, currentTimestamp))
        .orElseGet(
            () ->
                ValidationResult.invalid(
                    RpcErrorType.UNSUPPORTED_FORK, OSAKA.name() + " not configured"));
  }
}
