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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineGetBlobsV2 extends ExecutionEngineJsonRpcMethod
    implements StreamingJsonRpcMethod {
  private static final Logger LOG = LoggerFactory.getLogger(EngineGetBlobsV2.class);
  public static final int REQUEST_MAX_VERSIONED_HASHES = 128;
  // accumulate into a single buffer for ≤16 blobs to avoid one drain-wait per blob in the streamer
  private static final int SINGLE_WRITE_THRESHOLD = 16;
  private static final byte[] RESPONSE_CLOSE = new byte[] {']', '}'};

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
      writeNullResult(requestContext.getRequest().getId(), out, mapper);
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

    // Pre-scan: V2 returns null for the entire result if any blob is missing or unsupported
    final List<BlobProofBundle> validBundles = new ArrayList<>(versionedHashes.length);
    int missingBlobs = 0;
    int unsupportedBlobs = 0;
    for (final VersionedHash hash : versionedHashes) {
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
    availableCounter.inc(validBundles.size());

    LOG.debug(
        "Requested {} bundles, found {} valid bundles, {} missing, {} unsupported",
        versionedHashes.length,
        validBundles.size(),
        missingBlobs,
        unsupportedBlobs);

    if (missingBlobs > 0 || unsupportedBlobs > 0) {
      missCounter.inc();
      writeNullResult(requestContext.getRequest().getId(), out, mapper);
      return;
    }

    // pre-build all entries; parallelise only when multiple blobs (128KB each) offset ForkJoin
    // overhead
    final List<BlobAndProofV2> builtBundles =
        validBundles.size() > 2
            ? validBundles.parallelStream().map(this::createBlobAndProofV2).toList()
            : validBundles.stream().map(this::createBlobAndProofV2).toList();

    final byte[] header =
        ("{\"jsonrpc\":\"2.0\",\"id\":"
                + mapper.writeValueAsString(requestContext.getRequest().getId())
                + ",\"result\":[")
            .getBytes(StandardCharsets.UTF_8);

    if (builtBundles.size() <= SINGLE_WRITE_THRESHOLD) {
      // Build full response into one buffer and send with Content-Length (not chunked) to avoid
      // both drain-wait overhead and chunked transfer encoding framing cost.
      if (out instanceof JsonResponseStreamer jrs) {
        // Write directly into a Vert.x Buffer to avoid the ByteArrayOutputStream→byte[]→Buffer
        // copies that Buffer.buffer(byte[]) would introduce.
        final Buffer buf = Buffer.buffer(header.length + builtBundles.size() * 275_000 + 2);
        final var bufOut = new JsonResponseStreamer.VertxBufferOutputStream(buf);
        bufOut.write(header);
        for (int i = 0; i < builtBundles.size(); i++) {
          if (i > 0) bufOut.write(',');
          mapper.writeValue(bufOut, builtBundles.get(i));
        }
        bufOut.write(RESPONSE_CLOSE);
        jrs.writeAndClose(buf);
      } else {
        final ByteArrayOutputStream fullBuf = new ByteArrayOutputStream(16 * 1024);
        fullBuf.write(header);
        for (int i = 0; i < builtBundles.size(); i++) {
          if (i > 0) fullBuf.write(',');
          mapper.writeValue(fullBuf, builtBundles.get(i));
        }
        fullBuf.write(RESPONSE_CLOSE);
        fullBuf.writeTo(out);
      }
    } else {
      out.write(header);
      final ByteArrayOutputStream blobBuf = new ByteArrayOutputStream(285_000);
      for (int i = 0; i < builtBundles.size(); i++) {
        blobBuf.reset();
        if (i > 0) blobBuf.write(',');
        mapper.writeValue(blobBuf, builtBundles.get(i));
        blobBuf.writeTo(out);
      }
      out.write(RESPONSE_CLOSE);
    }
    hitCounter.inc();
  }

  private static void writeNullResult(
      final Object id, final OutputStream out, final ObjectMapper mapper) throws IOException {
    out.write(
        ("{\"jsonrpc\":\"2.0\",\"id\":" + mapper.writeValueAsString(id) + ",\"result\":null}")
            .getBytes(StandardCharsets.UTF_8));
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
        blobProofBundle.getKzgProof().stream()
            .map(proof -> HexUtils.toFastHex(proof.getData(), true))
            .toList());
  }

  @Override
  protected ValidationResult<RpcErrorType> validateForkSupported(final long currentTimestamp) {
    return ForkSupportHelper.validateForkSupported(OSAKA, osakaMilestone, currentTimestamp);
  }
}
