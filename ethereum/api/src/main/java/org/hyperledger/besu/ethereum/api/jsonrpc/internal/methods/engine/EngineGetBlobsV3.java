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
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonResponseStreamer;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.StreamingJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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
public class EngineGetBlobsV3 extends ExecutionEngineJsonRpcMethod
    implements StreamingJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV3.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;
  // accumulate into a single buffer for ≤16 blobs to avoid one drain-wait per blob in the streamer
  private static final int SINGLE_WRITE_THRESHOLD = 16;
  private static final byte[] RESPONSE_CLOSE = new byte[] {']', '}'};

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

    // Streaming-only method: single requests are routed to streamResponse() by the executor.
    // This path is only reachable if the method is included in a JSON-RPC batch request,
    // which cannot support streaming — returning INVALID_REQUEST mirrors the default behaviour
    // of StreamingJsonRpcMethod.response() for non-engine streaming methods.
    return new JsonRpcErrorResponse(
        requestContext.getRequest().getId(), RpcErrorType.INVALID_REQUEST);
  }

  @Override
  public void streamResponse(
      final JsonRpcRequestContext requestContext, final OutputStream out, final ObjectMapper mapper)
      throws IOException {
    if (mergeContext.get().isSyncing()) {
      out.write(
          ("{\"jsonrpc\":\"2.0\",\"id\":"
                  + mapper.writeValueAsString(requestContext.getRequest().getId())
                  + ",\"result\":null}")
              .getBytes(StandardCharsets.UTF_8));
      return;
    }
    final VersionedHash[] versionedHashes = extractVersionedHashes(requestContext);
    if (versionedHashes.length > REQUEST_MAX_VERSIONED_HASHES) {
      mapper.writeValue(
          out,
          new JsonRpcErrorResponse(
              requestContext.getRequest().getId(),
              RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST));
      return;
    }
    final long timestamp = protocolContext.getBlockchain().getChainHeadHeader().getTimestamp();
    final ValidationResult<RpcErrorType> forkValidationResult = validateForkSupported(timestamp);
    if (!forkValidationResult.isValid()) {
      mapper.writeValue(
          out, new JsonRpcErrorResponse(requestContext.getRequest().getId(), forkValidationResult));
      return;
    }

    requestedCounter.inc(versionedHashes.length);

    // pre-build all entries; parallelise encoding when multiple blobs offset ForkJoin overhead
    final List<BlobAndProofV2> results = getBlobV3Result(versionedHashes);
    final long availableCount = results.stream().filter(Objects::nonNull).count();

    final byte[] header =
        ("{\"jsonrpc\":\"2.0\",\"id\":"
                + mapper.writeValueAsString(requestContext.getRequest().getId())
                + ",\"result\":[")
            .getBytes(StandardCharsets.UTF_8);

    if (results.size() <= SINGLE_WRITE_THRESHOLD) {
      // Build full response into one buffer and send with Content-Length (not chunked) to avoid
      // both drain-wait overhead and chunked transfer encoding framing cost.
      if (out instanceof JsonResponseStreamer jrs) {
        // Write directly into a Vert.x Buffer to avoid the ByteArrayOutputStream→byte[]→Buffer
        // copies that Buffer.buffer(byte[]) would introduce.
        final Buffer buf = Buffer.buffer(header.length + results.size() * 275_000 + 2);
        final var bufOut = new JsonResponseStreamer.VertxBufferOutputStream(buf);
        bufOut.write(header);
        for (int i = 0; i < results.size(); i++) {
          if (i > 0) bufOut.write(',');
          final BlobAndProofV2 entry = results.get(i);
          if (entry == null) {
            bufOut.write("null".getBytes(StandardCharsets.UTF_8));
          } else {
            mapper.writeValue(bufOut, entry);
          }
        }
        bufOut.write(RESPONSE_CLOSE);
        jrs.writeAndClose(buf);
      } else {
        final ByteArrayOutputStream fullBuf = new ByteArrayOutputStream(16 * 1024);
        fullBuf.write(header);
        for (int i = 0; i < results.size(); i++) {
          if (i > 0) fullBuf.write(',');
          final BlobAndProofV2 entry = results.get(i);
          if (entry == null) {
            fullBuf.write("null".getBytes(StandardCharsets.UTF_8));
          } else {
            mapper.writeValue(fullBuf, entry);
          }
        }
        fullBuf.write(RESPONSE_CLOSE);
        fullBuf.writeTo(out);
      }
    } else {
      out.write(header);
      final ByteArrayOutputStream entryBuf = new ByteArrayOutputStream(285_000);
      for (int i = 0; i < results.size(); i++) {
        entryBuf.reset();
        if (i > 0) entryBuf.write(',');
        final BlobAndProofV2 entry = results.get(i);
        if (entry == null) {
          entryBuf.write("null".getBytes(StandardCharsets.UTF_8));
        } else {
          mapper.writeValue(entryBuf, entry);
        }
        entryBuf.writeTo(out);
      }
      out.write(RESPONSE_CLOSE);
    }

    availableCounter.inc(availableCount);
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
    // Blob pool lookups are sequential (single lock); encoding is parallelised when > 2 blobs
    // because each 128 KB blob takes ~260 µs to hex-encode and offsets ForkJoin overhead.
    final BlobProofBundle[] bundles = new BlobProofBundle[versionedHashes.length];
    for (int i = 0; i < versionedHashes.length; i++) {
      bundles[i] = transactionPool.getBlobProofBundle(versionedHashes[i]);
    }
    if (versionedHashes.length > 2) {
      return Arrays.stream(bundles).parallel().map(this::getBlobAndProofV2).toList();
    }
    return Arrays.stream(bundles).map(this::getBlobAndProofV2).toList();
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
        blobProofBundle.getKzgProof().stream()
            .map(proof -> HexUtils.toFastHex(proof.getData(), true))
            .toList());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateForkSupported(final long currentTimestamp) {
    return ForkSupportHelper.validateForkSupported(OSAKA, osakaMilestone, currentTimestamp);
  }
}
