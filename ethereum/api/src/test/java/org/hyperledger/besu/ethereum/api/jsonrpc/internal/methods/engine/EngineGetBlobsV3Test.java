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
import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.OSAKA;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV2;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
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
public class EngineGetBlobsV3Test extends AbstractScheduledApiTest {
  @Mock private BlockHeader blockHeader;
  @Mock private MutableBlockchain blockchain;
  @Mock private MergeContext mergeContext;

  private TransactionPool transactionPool;
  private EngineGetBlobsV3<?> method;

  private final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();

  @BeforeEach
  public void setup() {
    transactionPool = mock(TransactionPool.class);
    ProtocolContext protocolContext = mock(ProtocolContext.class);
    when(mergeContext.isSyncing()).thenReturn(false);
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.ofNullable(mergeContext));
    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);

    method =
        new EngineGetBlobsV3<>(
            protocolSchedule,
            protocolContext,
            mock(Vertx.class),
            mock(EngineCallListener.class),
            transactionPool,
            metricsSystem,
            OSAKA,
            null);
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
                bundle1.getVersionedHash(), unknownHash, bundle3.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isNotNull();
    assertThat(result.get(1)).isNull();
    assertThat(result.get(2)).isNotNull();
  }

  @Test
  public void shouldReturnNullForKzgProofBlobType() {
    BlobProofBundle bundle = createBundleWithBlobType(KZG_PROOF);
    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isNull();
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
    assertThat(result.get(0)).isNotNull();
    assertThat(result.get(1)).isNotNull();
    assertThat(result.get(2)).isNull();
    assertThat(result.get(3)).isNull();
    assertThat(result.get(4)).isNotNull();
  }

  @Test
  public void shouldReturnErrorForTooLargeRequest() {
    VersionedHash[] tooManyHashes = new VersionedHash[129];
    Arrays.fill(tooManyHashes, new VersionedHash((byte) 1, Hash.ZERO));

    JsonRpcResponse response = method.syncResponse(buildRequestContext(tooManyHashes));

    assertThat(fromErrorResp(response).getCode())
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
  public void shouldReturnNullsWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    BlobProofBundle bundle = createBundleWithBlobType(KZG_CELL_PROOFS);

    JsonRpcSuccessResponse response =
        getSuccessResponse(buildRequestContext(bundle.getVersionedHash()));

    @SuppressWarnings("unchecked")
    List<BlobAndProofV2> result = (List<BlobAndProofV2>) response.getResult();
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isNull();
  }

  @Test
  public void shouldSupportMinimum128Hashes() {
    VersionedHash[] maxHashes = new VersionedHash[128];
    Arrays.fill(maxHashes, new VersionedHash((byte) 1, Hash.ZERO));

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
    assertThat(result.getFirst().getBlob().getData()).isEqualTo(expected.getBlob().getData());
    assertThat(result.getFirst().getProofs()).hasSize(expected.getKzgProof().size());
  }
}
