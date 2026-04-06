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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine.EngineTestSupport.fromErrorResp;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.merge.MergeContext;
import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SECPPrivateKey;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequest;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlobAndProofV1;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.plugin.services.rpc.RpcResponseType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class EngineGetBlobsV1Test extends AbstractScheduledApiTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final SECPPrivateKey PRIVATE_KEY1 =
      SIGNATURE_ALGORITHM.createPrivateKey(
          Bytes32.fromHexString(
              "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"));
  private static final KeyPair KEYS1 =
      new KeyPair(PRIVATE_KEY1, SIGNATURE_ALGORITHM.createPublicKey(PRIVATE_KEY1));
  public static final VersionedHash VERSIONED_HASH_ZERO = new VersionedHash((byte) 1, Hash.ZERO);

  @Mock private ProtocolContext protocolContext;
  @Mock private EngineCallListener engineCallListener;
  @Mock private MutableBlockchain blockchain;
  @Mock private TransactionPool transactionPool;
  @Mock private BlockHeader blockHeader;
  @Mock private MergeContext mergeContext;

  private EngineGetBlobsV1 method;

  private static final Vertx vertx = Vertx.vertx();

  @BeforeEach
  public void beforeEach() {
    when(mergeContext.isSyncing()).thenReturn(false);
    when(protocolContext.safeConsensusContext(any())).thenReturn(Optional.ofNullable(mergeContext));

    when(protocolContext.getBlockchain()).thenReturn(blockchain);
    when(blockHeader.getTimestamp()).thenReturn(cancunHardfork.milestone());
    when(blockchain.getChainHeadHeader()).thenReturn(blockHeader);
    this.method =
        spy(
            new EngineGetBlobsV1(
                vertx, protocolContext, protocolSchedule, engineCallListener, transactionPool));
  }

  @Test
  public void shouldReturnExpectedMethodName() {
    assertThat(method.getName()).isEqualTo("engine_getBlobsV1");
  }

  @Test
  public void shouldReturnBlobsAndProofsForKnownVersionedHashesFromMap() {
    final Transaction blobTransaction = createBlobTransaction();

    final BlobsWithCommitments blobsWithCommitments =
        blobTransaction.getBlobsWithCommitments().get();

    mockTransactionPoolMethod(blobsWithCommitments);

    VersionedHash[] versionedHashes =
        blobsWithCommitments.getVersionedHashes().toArray(new VersionedHash[0]);

    final JsonRpcResponse jsonRpcResponse = resp(versionedHashes);

    final List<BlobAndProofV1> blobAndProofV1s = fromSuccessResp(jsonRpcResponse);

    assertThat(blobAndProofV1s.size()).isEqualTo(versionedHashes.length);
    // for loop to check each blob and proof
    for (int i = 0; i < versionedHashes.length; i++) {
      assertThat(Bytes.fromHexString(blobAndProofV1s.get(i).getBlob()))
          .isEqualTo(blobsWithCommitments.getBlobProofBundles().get(i).getBlob().getData());
      assertThat(blobsWithCommitments.getBlobProofBundles().get(i).getKzgProof().size())
          .isEqualTo(1);
      assertThat(Bytes.fromHexString(blobAndProofV1s.get(i).getProof()))
          .isEqualTo(
              blobsWithCommitments.getBlobProofBundles().get(i).getKzgProof().getFirst().getData());
    }
  }

  @Test
  public void shouldReturnNullForBlobsAndProofsForUnknownVersionedHashes() {
    final Transaction blobTransaction = createBlobTransaction();

    final BlobsWithCommitments blobsWithCommitments =
        blobTransaction.getBlobsWithCommitments().get();

    mockTransactionPoolMethod(blobsWithCommitments);

    List<VersionedHash> versionedHashesList =
        blobsWithCommitments.getVersionedHashes().stream().toList();

    final VersionedHash[] hashes = versionedHashesList.toArray(new VersionedHash[0]);
    hashes[1] = VERSIONED_HASH_ZERO;

    final JsonRpcResponse jsonRpcResponse = resp(hashes);

    final List<BlobAndProofV1> blobAndProofV1s = fromSuccessResp(jsonRpcResponse);

    assertThat(blobAndProofV1s.size()).isEqualTo(versionedHashesList.size());
    // for loop to check each blob and proof
    for (int i = 0; i < versionedHashesList.size(); i++) {
      if (i != 1) {
        assertThat(Bytes.fromHexString(blobAndProofV1s.get(i).getBlob()))
            .isEqualTo(blobsWithCommitments.getBlobProofBundles().get(i).getBlob().getData());
        assertThat(blobsWithCommitments.getBlobProofBundles().get(i).getKzgProof().size())
            .isEqualTo(1);
        assertThat(Bytes.fromHexString(blobAndProofV1s.get(i).getProof()))
            .isEqualTo(
                blobsWithCommitments
                    .getBlobProofBundles()
                    .get(i)
                    .getKzgProof()
                    .getFirst()
                    .getData());
      } else {
        assertThat(blobAndProofV1s.get(i)).isNull();
      }
    }
  }

  @Test
  public void shouldReturnOnlyNullsForBlobsAndProofsIfAllVersionedHashesUnknown() {
    final Transaction blobTransaction = createBlobTransaction();

    final BlobsWithCommitments blobsWithCommitments =
        blobTransaction.getBlobsWithCommitments().get();

    mockTransactionPoolMethod(blobsWithCommitments);

    List<VersionedHash> versionedHashesList =
        blobsWithCommitments.getVersionedHashes().stream().toList();

    final VersionedHash[] versionedHashes = new VersionedHash[6];
    Arrays.fill(versionedHashes, VERSIONED_HASH_ZERO);
    final JsonRpcResponse jsonRpcResponse = resp(versionedHashes);

    final List<BlobAndProofV1> blobAndProofV1s = fromSuccessResp(jsonRpcResponse);

    assertThat(blobAndProofV1s.size()).isEqualTo(versionedHashesList.size());
    // for loop to check each blob and proof
    for (int i = 0; i < versionedHashes.length; i++) {
      assertThat(blobAndProofV1s.get(i)).isNull();
    }
  }

  @Test
  public void shouldReturnEmptyResponseForEmptyRequest() {
    final VersionedHash[] versionedHashes = new VersionedHash[0];

    final JsonRpcResponse jsonRpcResponse = resp(versionedHashes);

    assertThat(fromSuccessResp(jsonRpcResponse).size()).isEqualTo(0);
  }

  @Test
  public void shouldFailWhenRequestingMoreThan128() {
    final VersionedHash[] versionedHashes = new VersionedHash[129];
    for (int i = 0; i < 129; i++) {
      versionedHashes[i] = new VersionedHash((byte) 1, Hash.ZERO);
    }

    final JsonRpcResponse jsonRpcResponse = resp(versionedHashes);

    assertThat(fromErrorResp(jsonRpcResponse).getCode())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getCode());
    assertThat(fromErrorResp(jsonRpcResponse).getMessage())
        .isEqualTo(RpcErrorType.INVALID_ENGINE_GET_BLOBS_TOO_LARGE_REQUEST.getMessage());
  }

  @Test
  void shouldFailWhenCancunNotActive() {
    when(blockHeader.getTimestamp()).thenReturn(cancunHardfork.milestone() - 1);
    var response = resp(new VersionedHash[] {VERSIONED_HASH_ZERO});
    assertThat(fromErrorResp(response).getCode())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }

  @Test
  void shouldSucceedWhenCancunActive() {
    when(blockHeader.getTimestamp()).thenReturn(cancunHardfork.milestone());
    var response = resp(new VersionedHash[] {VERSIONED_HASH_ZERO});
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  @Test
  void shouldSucceedWhenOsakaNotActive() {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone() - 1);
    var response = resp(new VersionedHash[] {VERSIONED_HASH_ZERO});
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
  }

  @Test
  void shouldFailWhenOsakaActive() {
    when(blockHeader.getTimestamp()).thenReturn(osakaHardfork.milestone());
    var response = resp(new VersionedHash[] {VERSIONED_HASH_ZERO});
    assertThat(fromErrorResp(response).getCode())
        .isEqualTo(RpcErrorType.UNSUPPORTED_FORK.getCode());
  }

  @Test
  public void shouldReturnNullWhenSyncing() {
    when(mergeContext.isSyncing()).thenReturn(true);
    var response = resp(new VersionedHash[] {VERSIONED_HASH_ZERO});
    assertThat(response.getType()).isEqualTo(RpcResponseType.SUCCESS);
    final List<BlobAndProofV1> blobAndProofV1s = fromSuccessResp(response);
    assertThat(blobAndProofV1s).hasSize(1);
    assertThat(blobAndProofV1s.getFirst()).isNull();
  }

  Transaction createBlobTransaction() {
    BlobTestFixture blobTestFixture = new BlobTestFixture();
    BlobsWithCommitments bwc = blobTestFixture.createBlobsWithCommitments(6);
    TransactionTestFixture ttf = new TransactionTestFixture();
    Transaction fullOfBlobs =
        ttf.to(Optional.of(Address.ZERO))
            .type(TransactionType.BLOB)
            .chainId(Optional.of(BigInteger.valueOf(42)))
            .gasLimit(21000)
            .maxFeePerGas(Optional.of(Wei.of(15)))
            .maxFeePerBlobGas(Optional.of(Wei.of(128)))
            .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
            .versionedHashes(Optional.of(bwc.getVersionedHashes()))
            .blobsWithCommitments(Optional.of(bwc))
            .createTransaction(KEYS1);
    return fullOfBlobs;
  }

  private void mockTransactionPoolMethod(final BlobsWithCommitments blobsWithCommitments) {
    blobsWithCommitments
        .getBlobProofBundles()
        .forEach(
            blobProofBundle ->
                when(transactionPool.getBlobProofBundle(blobProofBundle.getVersionedHash()))
                    .thenReturn(blobProofBundle));
  }

  private JsonRpcResponse resp(final VersionedHash[] versionedHashes) {
    return method.response(
        new JsonRpcRequestContext(
            new JsonRpcRequest(
                "2.0",
                RpcMethod.ENGINE_GET_BLOBS_V1.getMethodName(),
                new Object[] {versionedHashes})));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private List<BlobAndProofV1> fromSuccessResp(final JsonRpcResponse resp) {
    assertThat(resp.getType()).isEqualTo(RpcResponseType.SUCCESS);
    final List list =
        Optional.of(resp)
            .map(JsonRpcSuccessResponse.class::cast)
            .map(JsonRpcSuccessResponse::getResult)
            .map(List.class::cast)
            .get();
    final ArrayList<BlobAndProofV1> blobAndProofV1s = new ArrayList<>();
    list.forEach(obj -> blobAndProofV1s.add((BlobAndProofV1) obj));
    return blobAndProofV1s;
  }
}
