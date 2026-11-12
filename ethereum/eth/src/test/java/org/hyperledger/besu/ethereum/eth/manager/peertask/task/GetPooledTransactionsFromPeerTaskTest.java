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
package org.hyperledger.besu.ethereum.eth.manager.peertask.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.core.transactions.DevP2PUtils.createPooledTransactionsMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.CellsOnlyBlobTransactionFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.FrameTransactionEncoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.Blob;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.eth.core.transactions.TransactionAnnouncements;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.peertask.InvalidPeerTaskResponseException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.MalformedRlpFromPeerException;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerSelector;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutor;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResponseCode;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskExecutorResult;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskRequestSender;
import org.hyperledger.besu.ethereum.eth.manager.peertask.PeerTaskValidationResponse;
import org.hyperledger.besu.ethereum.eth.messages.PooledTransactionsMessage;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionAnnouncement;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GetPooledTransactionsFromPeerTaskTest extends TrustedSetupClassLoaderExtension {
  private static final BlockDataGenerator GENERATOR = new BlockDataGenerator();
  private static final Set<Capability> AGREED_CAPABILITIES = Set.of(EthProtocol.LATEST);

  @Test
  public void testGetRequestMessage() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    MessageData result = task.getRequestMessage(AGREED_CAPABILITIES);

    Assertions.assertEquals(
        "0xe1a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        result.getData().toHexString());
  }

  @Test
  public void testProcessResponse()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = GENERATOR.transaction();
    PooledTransactionsMessage pooledTransactionsMessage =
        createPooledTransactionsMessage(List.of(transaction));

    List<Transaction> result = task.processResponse(pooledTransactionsMessage, AGREED_CAPABILITIES);

    Assertions.assertEquals(List.of(transaction), result);
  }

  @Test
  public void testProcessResponseWithIncorrectTransactionCount() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    PooledTransactionsMessage pooledTransactionsMessage =
        createPooledTransactionsMessage(List.of(GENERATOR.transaction(), GENERATOR.transaction()));

    // More transactions than requested hashes is rejected while counting, before decoding
    final MalformedRlpFromPeerException exception =
        Assertions.assertThrows(
            MalformedRlpFromPeerException.class,
            () -> task.processResponse(pooledTransactionsMessage, AGREED_CAPABILITIES));

    Assertions.assertInstanceOf(RLPException.class, exception.getCause());
    Assertions.assertTrue(
        exception.getCause().getMessage().contains("exceeds the maximum permitted size"),
        exception.getCause().getMessage());
    Assertions.assertEquals(pooledTransactionsMessage.getData(), exception.getMessageData());
  }

  @Test
  public void testProcessResponseWithFewerTransactionsThanRequested()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    // A peer may legitimately not have every requested transaction any more
    final List<Hash> hashes = List.of(Hash.EMPTY, Hash.EMPTY_TRIE_HASH);
    final GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);
    final Transaction transaction = GENERATOR.transaction();

    final List<Transaction> result =
        task.processResponse(
            createPooledTransactionsMessage(List.of(transaction)), AGREED_CAPABILITIES);

    Assertions.assertEquals(List.of(transaction), result);
  }

  @Test
  public void testProcessResponseRejectsTheElidedFormFromAPreEth72Peer() throws Exception {
    // Eliding the blob payloads is an eth/72 answer. From a peer that negotiated anything earlier
    // it is a malformed response, which is what the executor needs to be told so the peer is
    // charged for it, rather than an encoder failure logged far from here.
    final Transaction transaction = new CellsOnlyBlobTransactionFixture().create(1, CellMask.FULL);
    final GetPooledTransactionsFromPeerTask task =
        new GetPooledTransactionsFromPeerTask(List.of(transaction.getHash()));

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(
        List.of(transaction),
        (tx, rlp) ->
            TransactionEncoder.encodeRLP(tx, rlp, EncodingContext.POOLED_TRANSACTION_ETH_72));
    final PooledTransactionsMessage message = PooledTransactionsMessage.createUnsafe(out.encoded());

    // Named explicitly rather than taken from AGREED_CAPABILITIES, which is EthProtocol.LATEST:
    // that is eth/71 only until eth/72 is advertised, after which this would assert nothing.
    Assertions.assertThrows(
        MalformedRlpFromPeerException.class,
        () -> task.processResponse(message, Set.of(EthProtocolVersion.V71.getCapability())));
  }

  @Test
  public void testProcessResponseAcceptsAnEth72AnnouncedBlobTransaction()
      throws InvalidPeerTaskResponseException, MalformedRlpFromPeerException {
    // An eth/72 peer announces, and returns, the blob-elided form. Sizing the received
    // transaction by the pre-eth/72 pooled form instead would both mismatch and, for a
    // transaction we now hold only as cells, have no encoding at all.
    final Transaction transaction = new CellsOnlyBlobTransactionFixture().create(1, CellMask.FULL);
    final TransactionAnnouncement announcement =
        new TransactionAnnouncement(
            transaction.getHash(),
            transaction.getType(),
            (long) transaction.getSizeForEth72Announcement(),
            CellMask.FULL);
    final GetPooledTransactionsFromPeerTask task =
        GetPooledTransactionsFromPeerTask.fromAnnouncements(List.of(announcement));

    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.writeList(
        List.of(transaction),
        (tx, rlp) ->
            TransactionEncoder.encodeRLP(tx, rlp, EncodingContext.POOLED_TRANSACTION_ETH_72));

    final List<Transaction> result =
        task.processResponse(
            PooledTransactionsMessage.createUnsafe(out.encoded()),
            Set.of(EthProtocolVersion.V72.getCapability()));

    Assertions.assertEquals(List.of(transaction.getHash()), Transaction.toHashList(result));
  }

  @Test
  public void testValidateResult() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = Mockito.mock(Transaction.class);
    Mockito.when(transaction.getHash()).thenReturn(Hash.EMPTY);

    PeerTaskValidationResponse validationResponse = task.validateResult(List.of(transaction));
    Assertions.assertEquals(PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD, validationResponse);
  }

  @Test
  public void testValidateResultWithMismatchedResults() {
    List<Hash> hashes = List.of(Hash.EMPTY);
    GetPooledTransactionsFromPeerTask task = new GetPooledTransactionsFromPeerTask(hashes);

    Transaction transaction = Mockito.mock(Transaction.class);
    Mockito.when(transaction.getHash()).thenReturn(Hash.EMPTY_TRIE_HASH);

    PeerTaskValidationResponse validationResponse = task.validateResult(List.of(transaction));
    Assertions.assertEquals(
        PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY, validationResponse);
  }

  @Test
  public void testValidateResultAcceptsOrderedSubset() {
    final Transaction first = GENERATOR.transaction();
    final Transaction second = GENERATOR.transaction();
    final Transaction third = GENERATOR.transaction();
    final GetPooledTransactionsFromPeerTask task =
        GetPooledTransactionsFromPeerTask.fromAnnouncements(
            TransactionAnnouncements.create(List.of(first, second, third)));

    Assertions.assertEquals(
        PeerTaskValidationResponse.RESULTS_VALID_AND_GOOD,
        task.validateResult(List.of(first, third)));
  }

  @Test
  public void testValidateResultRejectsOutOfOrderTransactions() {
    final Transaction first = GENERATOR.transaction();
    final Transaction second = GENERATOR.transaction();
    final GetPooledTransactionsFromPeerTask task =
        GetPooledTransactionsFromPeerTask.fromAnnouncements(
            TransactionAnnouncements.create(List.of(first, second)));

    Assertions.assertEquals(
        PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY,
        task.validateResult(List.of(second, first)));
  }

  @Test
  public void testValidateResultRejectsDuplicateTransactions() {
    final Transaction first = GENERATOR.transaction();
    final Transaction second = GENERATOR.transaction();
    final GetPooledTransactionsFromPeerTask task =
        GetPooledTransactionsFromPeerTask.fromAnnouncements(
            TransactionAnnouncements.create(List.of(first, second)));

    Assertions.assertEquals(
        PeerTaskValidationResponse.RESULTS_DO_NOT_MATCH_QUERY,
        task.validateResult(List.of(first, first)));
  }

  @Test
  public void wrappedBlobFrameTransactionInResponseDecodesAndKeepsPeer() throws Exception {
    final BlobProofBundle blob =
        new BlobTestFixture().createBlobProofBundle(BlobType.KZG_CELL_PROOFS);
    final Transaction frameTransaction =
        new TransactionTestFixture()
            .type(TransactionType.FRAME)
            .versionedHashes(Optional.of(List.of(blob.getVersionedHash())))
            .createTransaction(SignatureAlgorithmFactory.getInstance().generateKeyPair());
    final GetPooledTransactionsFromPeerTask task =
        new GetPooledTransactionsFromPeerTask(List.of(frameTransaction.getHash()));

    final EthPeer peer = mock(EthPeer.class);
    when(peer.getAgreedCapabilities()).thenReturn(AGREED_CAPABILITIES);
    final PeerTaskRequestSender requestSender = mock(PeerTaskRequestSender.class);
    when(requestSender.sendRequest(any(), any(), eq(peer)))
        .thenReturn(
            PooledTransactionsMessage.createUnsafe(
                pooledTransactionsWithBlobWrapper(frameTransaction, blob)));

    final PeerTaskExecutorResult<List<Transaction>> result =
        new PeerTaskExecutor(mock(PeerSelector.class), requestSender, new NoOpMetricsSystem())
            .executeAgainstPeer(task, peer);

    verify(peer, never()).disconnect(any());
    assertThat(result.responseCode()).isEqualTo(PeerTaskExecutorResponseCode.SUCCESS);
    final Transaction received = result.result().orElseThrow().getFirst();
    assertThat(received.getHash()).isEqualTo(frameTransaction.getHash());
    assertThat(received.getBlobsWithCommitments().orElseThrow().getBlobProofBundles())
        .containsExactly(blob);
  }

  /**
   * Encodes a PooledTransactions body holding one frame transaction in the EIP-8141 blob network
   * form, {@code type || rlp([tx_payload_body, wrapper_version, blobs, commitments, cell_proofs])}.
   */
  private static Bytes pooledTransactionsWithBlobWrapper(
      final Transaction frameTransaction, final BlobProofBundle blob) {
    final BytesValueRLPOutput wrapper = new BytesValueRLPOutput();
    wrapper.startList();
    FrameTransactionEncoder.encode(frameTransaction, wrapper);
    wrapper.writeIntScalar(BlobType.KZG_CELL_PROOFS.getVersionId());
    wrapper.writeList(List.of(blob.getBlob().orElseThrow()), Blob::writeTo);
    wrapper.writeList(List.of(blob.getKzgCommitment()), KZGCommitment::writeTo);
    wrapper.writeList(blob.getKzgProof(), KZGProof::writeTo);
    wrapper.endList();

    final BytesValueRLPOutput message = new BytesValueRLPOutput();
    message.startList();
    message.writeBytes(
        Bytes.concatenate(Bytes.of(TransactionType.FRAME.getSerializedType()), wrapper.encoded()));
    message.endList();
    return message.encoded();
  }
}
