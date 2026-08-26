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
import static org.assertj.core.api.Assertions.catchThrowableOfType;
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
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobCellsAndProofsV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetBlobsV4Test extends AbstractScheduledApiTest {
  private static final Bytes FULL_BITARRAY = Bytes.repeat((byte) 0xFF, 16);

  @Mock private BlockHeader blockHeader;
  @Mock private MutableBlockchain blockchain;

  private TransactionPool transactionPool;
  private EngineGetBlobsV4 method;

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
    when(blockHeader.getTimestamp()).thenReturn(amsterdamHardfork.milestone());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v4_requested_total"),
            anyString()))
        .thenReturn(requestedCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v4_available_total"),
            anyString()))
        .thenReturn(availableCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_v4_partial_total"),
            anyString()))
        .thenReturn(partialResponseCounter);
    when(metricsSystem.createCounter(
            eq(BesuMetricCategory.RPC), eq("execution_engine_getblobs_v4_full_total"), anyString()))
        .thenReturn(fullResponseCounter);

    method =
        new EngineGetBlobsV4(
            mock(Vertx.class),
            protocolContext,
            protocolSchedule,
            mock(EngineCallListener.class),
            transactionPool,
            metricsSystem);
  }

  @Test
  public void shouldReturnMethodName() {
    assertThat(method.getName()).isEqualTo(RpcMethod.ENGINE_GET_BLOBS_V4.getMethodName());
  }

  @Test
  public void shouldReturnAllCellsForFullBitarray() {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getBlobCells()).hasSize(CKZG4844Helper.CELL_PROOFS_PER_BLOB);
    assertThat(result.getFirst().getProofs()).hasSize(CKZG4844Helper.CELL_PROOFS_PER_BLOB);

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(1);
    verify(fullResponseCounter).inc();
    verifyNoInteractions(partialResponseCounter);
  }

  @Test
  public void shouldReturnOnlySelectedCellsForPartialBitarray() {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);
    // select cell index 0 and cell index 127 only
    byte[] maskBytes = new byte[16];
    maskBytes[0] = 0x01;
    maskBytes[15] = (byte) 0x80;
    Bytes bitarray = Bytes.wrap(maskBytes);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bitarray, bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getBlobCells()).hasSize(2);
    assertThat(result.getFirst().getProofs()).hasSize(2);

    Bytes blobCells = bundle.getBlobCellsBytes().orElseThrow();
    int cellSize = blobCells.size() / CKZG4844Helper.CELL_PROOFS_PER_BLOB;
    String expectedCell0 = blobCells.slice(0, cellSize).toHexString();
    String expectedCell127 = blobCells.slice(127 * cellSize, cellSize).toHexString();
    assertThat(result.getFirst().getBlobCells()).containsExactly(expectedCell0, expectedCell127);
    assertThat(result.getFirst().getProofs())
        .containsExactly(
            bundle.getKzgProof().get(0).getData().toHexString(),
            bundle.getKzgProof().get(127).getData().toHexString());
  }

  @Test
  public void shouldReturnNullForMissingBlobsInPartialResponse() {
    BlobProofBundle bundle1 = createBundleWithBlobType(KZG_CELL_PROOFS);
    VersionedHash unknownHash = new VersionedHash((byte) 1, Hash.ZERO);
    BlobProofBundle bundle3 = createBundleWithBlobType(KZG_CELL_PROOFS);

    when(transactionPool.getBlobProofBundle(bundle1.getVersionedHash())).thenReturn(bundle1);
    when(transactionPool.getBlobProofBundle(unknownHash)).thenReturn(null);
    when(transactionPool.getBlobProofBundle(bundle3.getVersionedHash())).thenReturn(bundle3);

    JsonRpcSuccessResponse response =
        getSuccessResponse(
            buildRequestContext(
                FULL_BITARRAY,
                bundle1.getVersionedHash(),
                unknownHash,
                bundle3.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isNotNull();
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2)).isNotNull();

    verify(requestedCounter).inc(3);
    verify(availableCounter).inc(2);
    verify(partialResponseCounter).inc();
    verifyNoInteractions(fullResponseCounter);
  }

  @Test
  public void shouldReturnNullForKzgProofBlobType() {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_PROOF);
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isNull();

    verify(requestedCounter).inc(1);
    verify(availableCounter).inc(0);
    verify(partialResponseCounter).inc();
    verifyNoInteractions(fullResponseCounter);
  }

  @Test
  public void shouldReturnErrorForTooLargeRequest() {
    VersionedHash[] tooManyHashes = new VersionedHash[129]; // > 128 limit
    Arrays.fill(tooManyHashes, new VersionedHash((byte) 1, Hash.ZERO));

    JsonRpcResponse response =
        method.syncResponse(buildRequestContext(FULL_BITARRAY, tooManyHashes));

    assertThat(fromErrorResp(response).getCode())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
  }

  @Test
  public void shouldRejectWrongLengthIndicesBitarray() {
    VersionedHash hash = new VersionedHash((byte) 1, Hash.ZERO);
    JsonRpcRequestContext context = buildRequestContext(Bytes.repeat((byte) 0xFF, 15), hash);

    InvalidJsonRpcParameters exception =
        catchThrowableOfType(() -> method.syncResponse(context), InvalidJsonRpcParameters.class);

    assertThat(exception).isNotNull();
    assertThat(exception.getRpcErrorType()).isEqualTo(RpcErrorType.INVALID_INDICES_BITARRAY_PARAMS);
  }

  @Test
  public void shouldReturnNullWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    assertThat(response.getResult()).isNull();
    verifyNoInteractions(
        requestedCounter, availableCounter, partialResponseCounter, fullResponseCounter);
  }

  @Test
  public void shouldSupportMinimum128Hashes() {
    VersionedHash[] maxHashes = new VersionedHash[128];
    Arrays.fill(maxHashes, new VersionedHash((byte) 1, Hash.ZERO));

    JsonRpcResponse response = method.syncResponse(buildRequestContext(FULL_BITARRAY, maxHashes));
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  private BlobProofBundle createBundleWithBlobType(
      final org.hyperledger.besu.datatypes.BlobType blobType) {
    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobProofBundle bundle = blobTestFixture.createBlobProofBundle(blobType);
    when(transactionPool.getBlobProofBundle(bundle.getVersionedHash())).thenReturn(bundle);
    return bundle;
  }

  private JsonRpcRequestContext buildRequestContext(
      final Bytes indicesBitarray, final VersionedHash... hashes) {
    return new JsonRpcRequestContext(
        new JsonRpcRequest(
            "2.0",
            RpcMethod.ENGINE_GET_BLOBS_V4.getMethodName(),
            new Object[] {hashes, indicesBitarray}));
  }

  private JsonRpcSuccessResponse getSuccessResponse(final JsonRpcRequestContext request) {
    JsonRpcResponse response = method.syncResponse(request);
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    return (JsonRpcSuccessResponse) response;
  }
}
