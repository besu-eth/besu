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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.datatypes.BlobType.KZG_CELL_PROOFS;
import static org.hyperledger.besu.datatypes.BlobType.KZG_PROOF;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.StreamingJsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetBlobsV3Test extends AbstractScheduledApiTest {
  @Mock private BlockHeader blockHeader;
  @Mock private MutableBlockchain blockchain;

  private TransactionPool transactionPool;
  private EngineGetBlobsV3 method;

  @Mock Counter requestedCounter;
  @Mock Counter availableCounter;
  @Mock Counter partialResponseCounter;
  @Mock Counter fullResponseCounter;
  @Mock ObservableMetricsSystem metricsSystem;
  @Mock MergeContext mergeContext;

  @BeforeEach
  public void setup() {
    transactionPool = mock(TransactionPool.class);
    ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(mergeContext.isSyncing()).thenReturn(false);
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.ofNullable(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v3_requested_total"),
            anyString()))
        .thenReturn(requestedCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v3_available_total"),
            anyString()))
        .thenReturn(availableCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v3_partial_total"),
            anyString()))
        .thenReturn(partialResponseCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC), eq("execution_engine_getblobs_v3_full_total"), anyString()))
        .thenReturn(fullResponseCounter);

    method =
        new EngineGetBlobsV3(
            mock(Vertx.class),
            protocolContext,
            protocolSchedule,
            mock(EngineCallListener.class),
            transactionPool,
            metricsSystem);
  }

  @Test
  public void shouldReturnMethodName() {
    assertThat(method.getName()).isEqualTo(RpcMethod.ENGINE_GET_BLOBS_V3.getMethodName());
  }

  @Test
  public void shouldReturnValidBlobsWithKzgCellProofs() {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));
    assertSingleValidBlob(response, bundle);

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(1);
    verify(fullResponseCounter).inc();
    verifyNoInteractions(partialResponseCounter);
  }

  @Test
  public void shouldReturnNullForMissingBlobsInPartialResponse() {
    // V3 key feature: partial responses with null for missing blobs
    BlobProofBundle bundle1 = createBundleWithBlobType(KZG_CELL_PROOFS);
    VersionedHash unknownHash = new VersionedHash((byte) 1, Hash.ZERO);
    BlobProofBundle bundle3 = createBundleWithBlobType(KZG_CELL_PROOFS);

    when(transactionPool.getBlobProofBundle(bundle1.getVersionedHash())).thenReturn(bundle1);
    when(transactionPool.getBlobProofBundle(unknownHash)).thenReturn(null);
    when(transactionPool.getBlobProofBundle(bundle3.getVersionedHash())).thenReturn(bundle3);

    JsonRpcSuccessResponse response =
        getSuccessResponse(
            buildRequestContext(
                bundle1.getVersionedHash(), unknownHash, bundle3.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isNotNull(); // first blob found
    assertThat(result.get(1)).isNull(); // middle blob missing - KEY V3 BEHAVIOR
    assertThat(result.get(2)).isNotNull(); // third blob found

    verify(requestedCounter).inc(3);
    verify(availableCounter).inc(2); // only 2 available
    verify(partialResponseCounter).inc(); // partial response
    verifyNoInteractions(fullResponseCounter);
  }

  @Test
  public void shouldReturnNullForKzgProofBlobType() {
    // V3 rejects KZG_PROOF like V2 does
    BlobProofBundle bundle = createBundleWithBlobType(KZG_PROOF);
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isNull(); // KZG_PROOF not supported

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0); // 0 available due to unsupported type
    verify(partialResponseCounter).inc(); // partial response (0 out of 1)
    verifyNoInteractions(fullResponseCounter);
  }

  @Test
  public void shouldMaintainOrderInPartialResponse() {
    BlobProofBundle bundle1 = createBundleWithBlobType(KZG_CELL_PROOFS);
    BlobProofBundle bundle2 = createBundleWithBlobType(KZG_CELL_PROOFS);
    VersionedHash missing1 =
        new VersionedHash(
            (byte) 1,
            Hash.fromHexString(
                "0x0300000000000000000000000000000000000000000000000000000000000000"));
    VersionedHash missing2 =
        new VersionedHash(
            (byte) 1,
            Hash.fromHexString(
                "0x0400000000000000000000000000000000000000000000000000000000000000"));
    BlobProofBundle bundle5 = createBundleWithBlobType(KZG_CELL_PROOFS);

    // Manually setup mocks to override the automatic setup in createBundleWithBlobType
    when(transactionPool.getBlobProofBundle(bundle1.getVersionedHash())).thenReturn(bundle1);
    when(transactionPool.getBlobProofBundle(bundle2.getVersionedHash())).thenReturn(bundle2);
    when(transactionPool.getBlobProofBundle(missing1)).thenReturn(null);
    when(transactionPool.getBlobProofBundle(missing2)).thenReturn(null);
    when(transactionPool.getBlobProofBundle(bundle5.getVersionedHash())).thenReturn(bundle5);

    JsonRpcSuccessResponse response =
        getSuccessResponse(
            buildRequestContext(
                bundle1.getVersionedHash(),
                bundle2.getVersionedHash(),
                missing1,
                missing2,
                bundle5.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(5);
    assertThat(result.get(0)).isNotNull(); // bundle1
    assertThat(result.get(1)).isNotNull(); // bundle2
    assertThat(result.get(2)).isNull(); // missing1
    assertThat(result.get(3)).isNull(); // missing2
    assertThat(result.get(4)).isNotNull(); // bundle5

    verify(partialResponseCounter).inc();
  }

  @Test
  public void shouldReturnErrorForTooLargeRequest() {
    VersionedHash[] tooManyHashes = new VersionedHash[129]; // > 128 limit
    Arrays.fill(tooManyHashes, new VersionedHash((byte) 1, Hash.ZERO));

    JsonRpcResponse response = method.syncResponse(buildRequestContext(tooManyHashes));

    assertThat(fromErrorResp(response).getCode())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
  }

  @Test
  public void shouldReturnNullWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));

    assertThat(response.getResult()).isNull();
    // No metrics should be incremented when syncing
    verifyNoInteractions(
        requestedCounter, availableCounter, partialResponseCounter, fullResponseCounter);
  }

  @Test
  public void shouldSupportMinimum128Hashes() {
    // Test capacity requirement from spec
    VersionedHash[] maxHashes = new VersionedHash[128];
    Arrays.fill(maxHashes, new VersionedHash((byte) 1, Hash.ZERO));

    // Should not error for exactly 128 hashes
    JsonRpcResponse response = method.syncResponse(buildRequestContext(maxHashes));
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  private BlobProofBundle createBundleWithBlobType(
      final org.hyperledger.besu.datatypes.BlobType blobType) {
    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobProofBundle bundle = blobTestFixture.createBlobProofBundle(blobType);
    when(transactionPool.getBlobProofBundle(bundle.getVersionedHash())).thenReturn(bundle);
    return bundle;
  }

  private JsonRpcRequestContext buildRequestContext(final VersionedHash... hashes) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0", RpcMethod.ENGINE_GET_BLOBS_V3.getMethodName(), new Object[] {hashes}));
  }

  private JsonRpcSuccessResponse getSuccessResponse(final JsonRpcRequestContext request) {
    JsonRpcResponse response = method.syncResponse(request);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (JsonRpcSuccessResponse) response;
  }

  private void assertSingleValidBlob(
      final JsonRpcSuccessResponse response, final BlobProofBundle expected) {
    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isNotNull();
    assertThat(result.getFirst().getBlob()).isEqualTo(expected.getBlob().getData().toHexString());
    assertThat(result.getFirst().getProofs()).hasSize(expected.getKzgProof().size());
  }

  // ── Streaming tests ────────────────────────────────────────────────────────

  private final ObjectMapper streamMapper = new ObjectMapper().registerModule(new Jdk8Module());

  @Test
  public void shouldImplementStreamingJsonRpcMethod() {
    assertThat(method).isInstanceOf(StreamingJsonRpcMethod.class);
  }

  @Test
  public void streamResponse_shouldReturnValidBlobs() throws IOException {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(buildRequestContext(bundle.getVersionedHash()), out, streamMapper);

    final JsonNode response = streamMapper.readTree(out.toByteArray());
    assertThat(response.get("jsonrpc").asText()).isEqualTo("2.0");
    assertThat(response.has("error")).isFalse();

    final JsonNode result = response.get("result");
    assertThat(result.isArray()).isTrue();
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).get("blob").asText())
        .isEqualTo(bundle.getBlob().getData().toHexString());
  }

  @Test
  public void streamResponse_shouldReturnNullEntriesForMissingBlobs() throws IOException {
    BlobProofBundle bundle1 = createBundleWithBlobType(KZG_CELL_PROOFS);
    VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);
    BlobProofBundle bundle3 = createBundleWithBlobType(KZG_CELL_PROOFS);

    when(transactionPool.getBlobProofBundle(bundle1.getVersionedHash())).thenReturn(bundle1);
    when(transactionPool.getBlobProofBundle(unknown)).thenReturn(null);
    when(transactionPool.getBlobProofBundle(bundle3.getVersionedHash())).thenReturn(bundle3);

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(
        buildRequestContext(bundle1.getVersionedHash(), unknown, bundle3.getVersionedHash()),
        out,
        streamMapper);

    final JsonNode result = streamMapper.readTree(out.toByteArray()).get("result");
    assertThat(result.isArray()).isTrue();
    assertThat(result.size()).isEqualTo(3);
    assertThat(result.get(0).isNull()).isFalse();
    assertThat(result.get(1).isNull()).isTrue();
    assertThat(result.get(2).isNull()).isFalse();
  }

  @Test
  public void streamResponse_shouldReturnNullWhenSyncing() throws IOException {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(buildRequestContext(bundle.getVersionedHash()), out, streamMapper);

    final JsonNode response = streamMapper.readTree(out.toByteArray());
    assertThat(response.get("result").isNull()).isTrue();
  }

  @Test
  public void streamResponse_shouldReturnErrorWhenTooManyHashes() throws IOException {
    VersionedHash[] hashes = new VersionedHash[129];
    Arrays.fill(hashes, new VersionedHash((byte) 1, Hash.ZERO));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(buildRequestContext(hashes), out, streamMapper);

    final JsonNode response = streamMapper.readTree(out.toByteArray());
    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("code").asInt())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
  }

  @Test
  public void streamResponse_shouldReturnErrorWhenForkNotSupported() throws IOException {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone() - 1);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(buildRequestContext(), out, streamMapper);

    final JsonNode response = streamMapper.readTree(out.toByteArray());
    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("code").asInt())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }
}
