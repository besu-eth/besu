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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV1;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import jakarta.validation.constraints.NotNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * #### Specification
 *
 * <p>1. Given an array of blob versioned hashes client software **MUST** respond with an array of
 * `BlobAndProofV1` objects with matching versioned hashes, respecting the order of versioned hashes
 * in the input array.
 *
 * <p>2. Client software **MUST** place responses in the order given in the request, using `null`
 * for any missing blobs. For instance, if the request is `[A_versioned_hash, B_versioned_hash,
 * C_versioned_hash]` and client software has data for blobs `A` and `C`, but doesn't have data for
 * `B`, the response **MUST** be `[A, null, C]`.
 *
 * <p>3. Client software **MUST** support request sizes of at least 128 blob versioned hashes. The
 * client **MUST** return `-38004: Too large request` error if the number of requested blobs is too
 * large.
 *
 * <p>4. Client software **MAY** return an array of all `null` entries if syncing or otherwise
 * unable to serve blob pool data.
 *
 * <p>5. Callers **MUST** consider that execution layer clients may prune old blobs from their pool,
 * and will respond with `null` if a blob has been pruned.
 */
public sealed class EngineGetBlobsV1<BAP extends BlobAndProofV1>
    extends ExecutionEngineJsonRpcMethod permits EngineGetBlobsV2 {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV1.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;
  protected final TransactionPool transactionPool;
  protected final GetBlobsMetrics getBlobsMetrics;

  public EngineGetBlobsV1(
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
    return RpcMethod.ENGINE_GET_BLOBS_V1.getMethodName();
  }

  @Override
  public JsonRpcResponse syncResponse(final JsonRpcRequestContext requestContext) {
    final VersionedHash[] versionedHashes;
    try {
      versionedHashes = requestContext.getRequiredParameter(0, VersionedHash[].class);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new InvalidJsonRpcParameters(
          "Invalid versioned hashes parameter (index 0)",
          RpcErrorType.INVALID_VERSIONED_HASHES_PARAMS,
          e);
    }

    if (versionedHashes.length > REQUEST_MAX_VERSIONED_HASHES) {
      return new JsonRpcErrorResponse(
          requestContext.getRequest().getId(),
          RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST);
    }

    if (mergeContext.get().isSyncing()) {
      return new JsonRpcSuccessResponse(
          requestContext.getRequest().getId(), getEmptyResult(versionedHashes));
    }

    final long timestamp = protocolContext.getBlockchain().getChainHeadHeader().getTimestamp();
    ValidationResult<RpcErrorType> forkValidationResult = validateForkSupported(timestamp);
    if (!forkValidationResult.isValid()) {
      return new JsonRpcErrorResponse(requestContext.getRequest().getId(), forkValidationResult);
    }

    return new JsonRpcSuccessResponse(
        requestContext.getRequest().getId(), getBlobResult(versionedHashes));
  }

  protected List<BlobAndProofV1> getEmptyResult(final VersionedHash[] versionedHashes) {
    return Collections.nCopies(versionedHashes.length, null);
  }

  protected List<BAP> getBlobResult(final VersionedHash[] versionedHashes) {
    return getResultPartialMode(versionedHashes);
  }

  protected @NotNull List<BAP> getResultPartialMode(final VersionedHash[] versionedHashes) {
    return fetchedBlobsData(versionedHashes).blobProofBundles().stream()
        .map(this::getBlobAndProofResult)
        .toList();
  }

  private @Nullable BAP getBlobAndProofResult(final BlobProofBundle blobProofBundle) {
    if (blobProofBundle == null) {
      return null;
    }
    return getVersionSpecificBlobAndProofResult(blobProofBundle);
  }

  @SuppressWarnings("unchecked")
  protected BAP getVersionSpecificBlobAndProofResult(final BlobProofBundle blobProofBundle) {
    return (BAP) new BlobAndProofV1(blobProofBundle);
  }

  protected FetchedBlobsData fetchedBlobsData(final VersionedHash[] versionedHashes) {
    final ArrayList<BlobProofBundle> validBundles =
        new ArrayList<>(Collections.nCopies(versionedHashes.length, null));
    getBlobsMetrics.increaseRequested(versionedHashes.length);
    int unsupportedBlobs = 0;
    int missingBlobs = 0;
    int foundBlobs = 0;
    for (int i = 0; i < versionedHashes.length; i++) {
      final VersionedHash hash = versionedHashes[i];
      final BlobProofBundle bundle = transactionPool.getBlobProofBundle(hash);
      if (bundle == null) {
        LOG.trace("No BlobProofBundle found for versioned hash: {}", hash);
        missingBlobs++;
        continue;
      }
      if (isUnsupportedBlob(bundle)) {
        LOG.trace("Unsupported blob type {} for versioned hash: {}", bundle.getBlobType(), hash);
        unsupportedBlobs++;
        continue;
      }
      validBundles.set(i, bundle);
      foundBlobs++;
    }

    getBlobsMetrics.increaseAvailable(foundBlobs);
    getBlobsMetrics.increaseMissing(missingBlobs);
    getBlobsMetrics.increaseUnsupported(unsupportedBlobs);
    if (foundBlobs == versionedHashes.length) {
      getBlobsMetrics.increaseFull();
    } else {
      getBlobsMetrics.increasePartial();
    }

    LOG.debug(
        "Requested {} bundles, found {} valid bundles, {} missing, {} unsupported",
        versionedHashes.length,
        foundBlobs,
        missingBlobs,
        unsupportedBlobs);

    return new FetchedBlobsData(validBundles, missingBlobs + unsupportedBlobs > 0);
  }

  protected boolean isUnsupportedBlob(final BlobProofBundle blobProofBundle) {
    return blobProofBundle.getBlobType() == BlobType.KZG_CELL_PROOFS;
  }

  protected record FetchedBlobsData(List<BlobProofBundle> blobProofBundles, boolean partial) {}
}
