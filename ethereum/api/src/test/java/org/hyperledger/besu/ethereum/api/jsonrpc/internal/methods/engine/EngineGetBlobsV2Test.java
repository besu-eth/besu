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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.JsonRpcParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
            protocolSchedule,
            protocolContext,
            mock(Vertx.class),
            mock(EngineCallListener.class),
            transactionPool,
            metricsSystem);
  }

  @Test
  public void shouldReturnMethodName() {
    assertThat(method.getName()).isEqualTo(RpcMethod.ENGINE_GET_BLOBS_V2.getMethodName());
  }

  @Test
  public void shouldReturnValidBlobs() {
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));
    assertSingleValidBlob(response, bundle);

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(1);
    verify(hitCounter).inc();
    verifyNoInteractions(missCounter);
  }

  @Test
  public void shouldReturnNullForUnknownHash() {
    VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);
    JsonRpcSuccessResponse response = getSuccessResponse(buildRequestContext(unknown));
    List<BlobAndProofV2> result = extractResult(response);
    assertThat(result).isNull();

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldNotReturnPartialResults() {
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    VersionedHash known = bundle.getVersionedHash();
    VersionedHash unknown = new VersionedHash((byte) 1, Hash.ZERO);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(known, unknown, known));
    List<BlobAndProofV2> result = extractResult(response);

    assertThat(result).isNull();
    verify(requestedCounter).inc(3);
    verify(availableCounter).inc(2);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldReturnNullForBlobProofBundleWithV1BlobType() {
    BlobTestFixture blobFixture = new BlobTestFixture();
    BlobProofBundle v1Bundle = blobFixture.createBlobProofBundle(KZG_PROOF);
    VersionedHash versionedHash = v1Bundle.getVersionedHash();
    when(transactionPool.getBlobProofBundle(versionedHash)).thenReturn(v1Bundle);

    JsonRpcRequestContext requestContext = buildRequestContext(versionedHash);
    JsonRpcSuccessResponse response = getSuccessResponse(requestContext);
    List<BlobAndProofV2> result = extractResult(response);

    assertThat(result).isNull();
    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0);
    verify(missCounter).inc();
    verifyNoInteractions(hitCounter);
  }

  @Test
  public void shouldReturnEmptyListWhenNoHashesGiven() {
    JsonRpcSuccessResponse response = getSuccessResponse(buildRequestContext());
    List<BlobAndProofV2> result = extractResult(response);
    assertThat(result).isEmpty();
  }

  @Test
  public void shouldReturnErrorWhenTooManyHashesGiven() {
    VersionedHash[] hashes = new VersionedHash[129];
    Arrays.fill(hashes, new VersionedHash((byte) 1, Hash.ZERO));
    JsonRpcRequestContext context = buildRequestContext(hashes);
    JsonRpcResponse response = method.syncResponse(context);

    assertThat(response).isInstanceOf(JsonRpcErrorResponse.class);
    JsonRpcErrorResponse error = (JsonRpcErrorResponse) response;
    assertThat(error.getError().getCode())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
  }

  @Test
  void shouldFailWhenOsakaNotActive() {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone() - 1);
    var response = method.syncResponse(buildRequestContext());
    assertThat(fromErrorResp(response).getCode())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }

  @Test
  void shouldSucceedWhenOsakaActive() {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone());
    var response = method.syncResponse(buildRequestContext());
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  @Test
  public void shouldReturnNullWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleAndRegisterToPool();
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));
    assertThat(response.getResult()).isNull();
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

  private JsonRpcSuccessResponse getSuccessResponse(final JsonRpcRequestContext context) {
    JsonRpcResponse response = method.syncResponse(context);
    assertThat(response).isInstanceOf(JsonRpcSuccessResponse.class);
    return (JsonRpcSuccessResponse) response;
  }

  @SuppressWarnings("unchecked")
  private List<BlobAndProofV2> extractResult(final JsonRpcSuccessResponse response) {
    return (List<BlobAndProofV2>) response.getResult();
  }

  private void assertSingleValidBlob(
      final JsonRpcSuccessResponse response, final BlobProofBundle bundle) {
    List<BlobAndProofV2> result = extractResult(response);
    assertThat(result).hasSize(1);
    BlobAndProofV2 blob = result.getFirst();
    assertThat(blob).isNotNull();
    assertThat(blob.getBlob()).isEqualTo(bundle.getBlob().getData().toHexString());

    List<String> expectedProofs =
        bundle.getKzgProof().stream().map(p -> p.getData().toHexString()).toList();

    assertThat(blob.getProofs()).containsExactlyElementsOf(expectedProofs);
  }
}
