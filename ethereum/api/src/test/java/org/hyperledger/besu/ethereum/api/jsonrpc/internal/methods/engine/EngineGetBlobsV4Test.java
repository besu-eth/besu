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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcObjectMapperFactory;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.exception.InvalidJsonRpcParameters;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ConstructorArgumentsBuilder;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobCellsAndProofsV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.BesuMetricCategory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.plugin.services.metrics.Counter;
import org.hyperledger.besu.plugin.services.metrics.LabelledMetric;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
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

  /** Cell indexes 64 to 127, a plausible custody set and one that starts above zero. */
  private static final CellMask UPPER_HALF =
      CellMask.fromBytes(
          Bytes.concatenate(Bytes.repeat((byte) 0x00, 8), Bytes.repeat((byte) 0xFF, 8)));

  @Mock private BlockHeader blockHeader;
  @Mock private MutableBlockchain blockchain;

  private TransactionPool transactionPool;
  private EngineGetBlobsV4 method;

  // GetBlobsMetrics calls labelledMetric.labels(version) once per inc(...) call, so each
  // LabelledMetric mock must resolve .labels(...) to a fixed Counter mock: chained
  // verify(labelledMetric).labels(x).inc(y) does not reliably resolve the intermediate
  // labels(x) return value through Mockito's stub for a plain (non deep-stub) mock, so the
  // production code's actual Counter instance is captured here and verified against directly.
  @Mock LabelledMetric<Counter> requestedLabelledCounter;
  @Mock LabelledMetric<Counter> availableLabelledCounter;
  @Mock LabelledMetric<Counter> missingLabelledCounter;
  @Mock LabelledMetric<Counter> partialResponseLabelledCounter;
  @Mock LabelledMetric<Counter> fullResponseLabelledCounter;
  @Mock Counter requestedCounter;
  @Mock Counter availableCounter;
  @Mock Counter missingCounter;
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

    when(metricsSystem.createLabelledCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_requested_total"),
            anyString(),
            eq("version")))
        .thenReturn(requestedLabelledCounter);
    when(metricsSystem.createLabelledCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_available_total"),
            anyString(),
            eq("version")))
        .thenReturn(availableLabelledCounter);
    when(metricsSystem.createLabelledCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_missing_total"),
            anyString(),
            eq("version")))
        .thenReturn(missingLabelledCounter);
    when(metricsSystem.createLabelledCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_partial_total"),
            anyString(),
            eq("version")))
        .thenReturn(partialResponseLabelledCounter);
    when(metricsSystem.createLabelledCounter(
            eq(BesuMetricCategory.RPC),
            eq("execution_engine_getblobs_full_total"),
            anyString(),
            eq("version")))
        .thenReturn(fullResponseLabelledCounter);

    when(requestedLabelledCounter.labels(anyString())).thenReturn(requestedCounter);
    when(availableLabelledCounter.labels(anyString())).thenReturn(availableCounter);
    when(missingLabelledCounter.labels(anyString())).thenReturn(missingCounter);
    when(partialResponseLabelledCounter.labels(anyString())).thenReturn(partialResponseCounter);
    when(fullResponseLabelledCounter.labels(anyString())).thenReturn(fullResponseCounter);

    method =
        new EngineGetBlobsV4(
            new ConstructorArgumentsBuilder()
                .protocolSchedule(protocolSchedule)
                .protocolContext(protocolContext)
                .vertx(mock(Vertx.class))
                .engineCallListener(mock(EngineCallListener.class))
                .mergeCoordinator(mock(MergeMiningCoordinator.class))
                .transactionPool(transactionPool)
                .ethPeers(mock(EthPeers.class))
                .metricsSystem(metricsSystem)
                .maxRequestBlocks(0)
                .build(),
            OSAKA,
            null);
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
    verify(missingCounter).inc(0);
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
    Bytes expectedCell0 = blobCells.slice(0, cellSize);
    Bytes expectedCell127 = blobCells.slice(127 * cellSize, cellSize);
    assertThat(result.getFirst().getBlobCells()).containsExactly(expectedCell0, expectedCell127);
    assertThat(result.getFirst().getProofs())
        .containsExactly(bundle.getKzgProof().get(0), bundle.getKzgProof().get(127));
  }

  @Test
  public void shouldReturnTheRequestedCellsOfASparselyHeldBlob() {
    // Under eth/72 the pool may hold only some of a blob's cells. Reading them by cell index is not
    // the same as slicing the concatenation at index * cellSize, which is what a held-cell count of
    // 64 would make of cell index 64.
    final BlobProofBundle full = new BlobTestFixture().createBlobProofBundle(KZG_CELL_PROOFS);
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    final BlobProofBundle sparse = sparseCopyOf(full, UPPER_HALF);

    // request cell index 64 and cell index 127, both held
    final byte[] requested = new byte[16];
    requested[8] = 0x01;
    requested[15] = (byte) 0x80;

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(Bytes.wrap(requested), sparse.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getBlobCells())
        .containsExactly(allCells.getCell(64).getData(), allCells.getCell(127).getData());
    assertThat(result.getFirst().getProofs())
        .containsExactly(full.getKzgProof().get(64), full.getKzgProof().get(127));
  }

  @Test
  public void shouldReturnNullInPlaceOfARequestedCellThatIsNotHeld() throws Exception {
    // The spec: a cell unavailable for an otherwise available blob is null in blob_cells, and its
    // entry in proofs is null too, rather than the whole blob being null.
    final BlobProofBundle full = new BlobTestFixture().createBlobProofBundle(KZG_CELL_PROOFS);
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    final BlobProofBundle sparse = sparseCopyOf(full, UPPER_HALF);

    // request cell index 0, not held, and cell index 127, held
    final byte[] requested = new byte[16];
    requested[0] = 0x01;
    requested[15] = (byte) 0x80;

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(Bytes.wrap(requested), sparse.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getBlobCells())
        .containsExactly(null, allCells.getCell(127).getData());
    assertThat(result.getFirst().getProofs()).containsExactly(null, full.getKzgProof().get(127));

    // and the nulls survive serialization, in place
    final JsonNode json =
        JsonRpcObjectMapperFactory.getResponseMapper().valueToTree(result.getFirst());
    assertThat(json.get("blob_cells").get(0).isNull()).isTrue();
    assertThat(json.get("blob_cells").get(1).isTextual()).isTrue();
    assertThat(json.get("proofs").get(0).isNull()).isTrue();
    assertThat(json.get("proofs").get(1).isTextual()).isTrue();
  }

  // Which bundle answers a hash, and whether one does at all, is decided by the pool, which answers
  // null when it holds none of the requested cells; see BlobV1TransactionPoolTest. Here a null
  // simply becomes a null entry, as shouldReturnNullForMissingBlobsInPartialResponse covers.

  /** The same blob, holding only the cells of {@code mask}, as a sampling node would. */
  private BlobProofBundle sparseCopyOf(final BlobProofBundle full, final CellMask mask) {
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    final List<Cell> held = mask.streamIndexes().mapToObj(allCells::getCell).toList();
    final BlobProofBundle sparse =
        new BlobProofBundle(
            KZG_CELL_PROOFS,
            new CellsWithMask(held, mask),
            full.getKzgCommitment(),
            full.getKzgProof(),
            full.getVersionedHash());
    when(transactionPool.getBlobProofBundle(eq(sparse.getVersionedHash()), anyList()))
        .thenReturn(sparse);
    return sparse;
  }

  @Test
  public void shouldReturnNullForMissingBlobsInPartialResponse() {
    BlobProofBundle bundle1 = createBundleWithBlobType(KZG_CELL_PROOFS);
    VersionedHash unknownHash = new VersionedHash((byte) 1, Hash.ZERO);
    BlobProofBundle bundle3 = createBundleWithBlobType(KZG_CELL_PROOFS);

    when(transactionPool.getBlobProofBundle(eq(bundle1.getVersionedHash()), anyList()))
        .thenReturn(bundle1);
    when(transactionPool.getBlobProofBundle(eq(unknownHash), anyList())).thenReturn(null);
    when(transactionPool.getBlobProofBundle(eq(bundle3.getVersionedHash()), anyList()))
        .thenReturn(bundle3);

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
    verify(missingCounter).inc(1);
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
    verify(missingCounter).inc(1);
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
  void shouldReturnNullWhenOsakaNotActive() {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone() - 1);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    assertThat(response.getResult()).isNull();
    verifyNoInteractions(
        requestedCounter,
        availableCounter,
        missingCounter,
        partialResponseCounter,
        fullResponseCounter);
  }

  @Test
  void shouldServeCellsBeforeAmsterdam() {
    when(blockHeader.getTimestamp()).thenReturn(amsterdamHardfork.milestone() - 1);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobCellsAndProofsV1> result = (List<BlobCellsAndProofsV1>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getBlobCells()).hasSize(CKZG4844Helper.CELL_PROOFS_PER_BLOB);
  }

  @Test
  void shouldSucceedWhenAmsterdamActive() {
    when(blockHeader.getTimestamp()).thenReturn(amsterdamHardfork.milestone());
    JsonRpcResponse response =
        method.syncResponse(buildRequestContext(FULL_BITARRAY, new VersionedHash[0]));
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  @Test
  public void shouldReturnNullWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(FULL_BITARRAY, bundle.getVersionedHash()));

    assertThat(response.getResult()).isNull();
    verifyNoInteractions(
        requestedCounter,
        availableCounter,
        missingCounter,
        partialResponseCounter,
        fullResponseCounter);
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
    when(transactionPool.getBlobProofBundle(eq(bundle.getVersionedHash()), anyList()))
        .thenReturn(bundle);
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
