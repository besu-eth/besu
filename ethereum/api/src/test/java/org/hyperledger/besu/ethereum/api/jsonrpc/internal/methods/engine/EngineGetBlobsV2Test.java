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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
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
public class EngineGetBlobsV2Test extends AbstractScheduledApiTest {
  @Mock private BlockHeader blockHeader;
  @Mock private MutableBlockchain blockchain;

  private TransactionPool transactionPool;
  private EngineGetBlobsV2 method;

  @Mock Counter requestedCounter;
  @Mock Counter availableCounter;
  @Mock Counter hitCounter;
  @Mock Counter missCounter;
  @Mock ObservableMetricsSystem metricsSystem;
  @Mock MergeContext mergeContext;

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

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
            eq("execution_engine_getblobs_requested_total"),
            anyString()))
        .thenReturn(requestedCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_available_total"),
            anyString()))
        .thenReturn(availableCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC), eq("execution_engine_getblobs_hit_total"), anyString()))
        .thenReturn(hitCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC), eq("execution_engine_getblobs_miss_total"), anyString()))
        .thenReturn(missCounter);

    method =
        new EngineGetBlobsV2(
            mock(Vertx.class),
            protocolContext,
            protocolSchedule,
            mock(EngineCallListener.class),
            transactionPool,
            metricsSystem);
  }

  @Test
  public void shouldReturnMethodName() {
    assertThat(method.getName()).isEqualTo(RpcMethod.ENGINE_GET_BLOBS_V2.getMethodName());
  }

  @Test
  public void shouldImplementStreamingJsonRpcMethod() {
    assertThat(method).isInstanceOf(StreamingJsonRpcMethod.class);
  }

  @Test
  public void shouldReturnValidBlobs() throws IOException {
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    JsonNode result = streamSuccessResult(buildRequestContext(bundle.getVersionedHash()));

    assertThat(result.isArray()).isTrue();
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).get("blob").asText())
        .isEqualTo(bundle.getBlob().getData().toHexString());
    assertThat(result.get(0).get("proofs")).isNotNull();

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(1);
    verify(hitCounter).inc();
    verifyNoInteractions(missCounter);
  }

  @Test
  public void shouldReturnNullForUnknownHash() throws IOException {
    VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);
    JsonNode result = streamSuccessResult(buildRequestContext(unknown));
    assertThat(result.isNull()).isTrue();

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldNotReturnPartialResults() throws IOException {
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    VersionedHash known = bundle.getVersionedHash();
    VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);

    JsonNode result = streamSuccessResult(buildRequestContext(known, unknown, known));
    assertThat(result.isNull()).isTrue();

    verify(requestedCounter).inc(3);
    verify(availableCounter).inc(2);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldReturnNullForBlobProofBundleWithV1BlobType() throws IOException {
    BlobTestFixture blobFixture = new BlobTestFixture();
    BlobProofBundle v1Bundle = blobFixture.createBlobProofBundle(KZG_PROOF);
    VersionedHash versionedHash = v1Bundle.getVersionedHash();
    when(transactionPool.getBlobProofBundle(versionedHash)).thenReturn(v1Bundle);

    JsonNode result = streamSuccessResult(buildRequestContext(versionedHash));
    assertThat(result.isNull()).isTrue();

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldReturnEmptyListWhenNoHashesGiven() throws IOException {
    JsonNode result = streamSuccessResult(buildRequestContext());
    assertThat(result.isArray()).isTrue();
    assertThat(result.isEmpty()).isTrue();
  }

  @Test
  public void shouldReturnErrorWhenTooManyHashesGiven() throws IOException {
    VersionedHash[] hashes = new VersionedHash[129];
    Arrays.fill(hashes, new VersionedHash((byte) 1, Hash.ZERO));
    JsonNode response = stream(buildRequestContext(hashes));

    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("code").asInt())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
  }

  @Test
  void shouldFailWhenOsakaNotActive() throws IOException {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone() - 1);
    JsonNode response = stream(buildRequestContext());

    assertThat(response.has("error")).isTrue();
    assertThat(response.get("error").get("code").asInt())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }

  @Test
  void shouldSucceedWhenOsakaActive() throws IOException {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone());
    JsonNode response = stream(buildRequestContext());

    assertThat(response.has("error")).isFalse();
    assertThat(response.get("result").isArray()).isTrue();
  }

  @Test
  public void shouldReturnNullWhenSyncing() throws IOException {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    JsonNode result = streamSuccessResult(buildRequestContext(bundle.getVersionedHash()));
    assertThat(result.isNull()).isTrue();

    verifyNoInteractions(requestedCounter);
    verifyNoInteractions(availableCounter);
    verifyNoInteractions(missCounter);
    verifyNoInteractions(hitCounter);
  }

  private BlobProofBundle createBundleAndRegisterToPool() {
    BlobTestFixture blobFixture = new BlobTestFixture();
    BlobProofBundle bundle = blobFixture.createBlobProofBundle(KZG_CELL_PROOFS);
    when(transactionPool.getBlobProofBundle(bundle.getVersionedHash())).thenReturn(bundle);
    return bundle;
  }

  private JsonRpcRequestContext buildRequestContext(final VersionedHash... hashes) {
    JsonRpcRequestContext context = mock(JsonRpcRequestContext.class);
    try {
      when(context.getRequiredParameter(eq(0), eq(VersionedHash[].class))).thenReturn(hashes);
    } catch (JsonRpcParameter.JsonRpcParameterException e) {
      throw new RuntimeException(e);
    }
    when(context.getRequest())
        .thenReturn(new JsonRpcRequest("2.0", "engine_getBlobsV2", new Object[] {}));
    return context;
  }

  private JsonNode stream(final JsonRpcRequestContext context) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    method.streamResponse(context, out, mapper);
    return mapper.readTree(out.toByteArray());
  }

  private JsonNode streamSuccessResult(final JsonRpcRequestContext context) throws IOException {
    JsonNode response = stream(context);
    assertThat(response.has("error")).isFalse();
    return response.get("result");
  }
}
