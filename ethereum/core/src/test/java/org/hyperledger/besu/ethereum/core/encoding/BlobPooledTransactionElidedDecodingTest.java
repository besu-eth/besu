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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlobTestFixture;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.security.InvalidParameterException;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * Covers the eth/72 wire form in which a {@code PooledTransactions} response elides blob payloads,
 * encoding the {@code blobs} field as the empty list.
 */
class BlobPooledTransactionElidedDecodingTest extends TrustedSetupClassLoaderExtension {

  private static final KeyPair KEYS = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private static final int BLOB_COUNT = 3;

  private static Transaction blobTransaction() {
    final BlobsWithCommitments withBlobs =
        CKZG4844Helper.convertToVersion1(
            new BlobTestFixture().createBlobsWithCommitments(BLOB_COUNT));
    return Transaction.builder()
        .type(TransactionType.BLOB)
        .chainId(java.math.BigInteger.ONE)
        .nonce(1)
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(Wei.of(10))
        .maxFeePerBlobGas(Wei.of(10))
        .gasLimit(100_000)
        .to(org.hyperledger.besu.datatypes.Address.ZERO)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .versionedHashes(withBlobs.getVersionedHashes())
        .blobsWithCommitments(withBlobs)
        .signAndBuild(KEYS);
  }

  private static Transaction roundTripElided(final Transaction tx) {
    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.POOLED_TRANSACTION_ETH_72);
    return TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.POOLED_TRANSACTION_ETH_72);
  }

  @Test
  void decodesTransactionWhoseBlobsWereElided() {
    final Transaction tx = blobTransaction();
    final Transaction decoded = roundTripElided(tx);

    // Eliding the payloads must not change the transaction's identity.
    assertThat(decoded.getHash()).isEqualTo(tx.getHash());

    final BlobsWithCommitments bwc = decoded.getBlobsWithCommitments().orElseThrow();
    assertThat(bwc.getBlobType()).isEqualTo(BlobType.KZG_CELL_PROOFS);
    // One bundle per blob, i.e. per commitment. Sizing this from the proof count would give
    // 128x too many.
    assertThat(bwc.getBlobProofBundles()).hasSize(BLOB_COUNT);
    assertThat(bwc.getKzgCommitments())
        .isEqualTo(tx.getBlobsWithCommitments().get().getKzgCommitments());
  }

  @Test
  void aTransactionHeldOnlyAsCellsIsEncodableOnlyWhereBlobsAreNotWritten() {
    final Transaction withBlobs = blobTransaction();
    // Round-tripping through the eth/72 form is how a node ends up holding cells and no blobs.
    final Transaction asCells = roundTripElided(withBlobs);

    assertThat(withBlobs.getBlobsWithCommitments().orElseThrow().hasBlobData()).isTrue();
    assertThat(asCells.getBlobsWithCommitments().orElseThrow().hasBlobData()).isFalse();

    // A block body carries no sidecar, and eth/72 elides the payloads, so both encode either way.
    for (final EncodingContext context :
        List.of(EncodingContext.BLOCK_BODY, EncodingContext.POOLED_TRANSACTION_ETH_72)) {
      assertThat(context.canEncode(withBlobs)).isTrue();
      assertThat(context.canEncode(asCells)).isTrue();
    }

    // Only the pre-eth/72 pooled form writes the payloads, so only it can fail.
    assertThat(EncodingContext.POOLED_TRANSACTION.canEncode(withBlobs)).isTrue();
    assertThat(EncodingContext.POOLED_TRANSACTION.canEncode(asCells)).isFalse();
  }

  @Test
  void encodingACellsOnlyTransactionForAPreEth72PeerFailsLegibly() {
    // The backstop for anything that reaches the encoder without asking canEncode first: the blob
    // list is empty, so without this the encoder would write a malformed transaction, not fail.
    final Transaction asCells = roundTripElided(blobTransaction());

    assertThatExceptionOfType(InvalidParameterException.class)
        .isThrownBy(
            () ->
                TransactionEncoder.encodeOpaqueBytes(asCells, EncodingContext.POOLED_TRANSACTION));
  }

  @Test
  void elidesBlobsAsTheEmptyListNotTheEmptyString() {
    // EIP-8070 is explicit: the blobs field "MUST be encoded as the RLP empty list (0xc0)" and
    // "MUST NOT be encoded as the RLP empty byte string". The two are one byte apart and a decoder
    // reading the wrong one fails on every elided transaction, so pin the byte.
    final Bytes encoded =
        TransactionEncoder.encodeOpaqueBytes(
            blobTransaction(), EncodingContext.POOLED_TRANSACTION_ETH_72);

    assertThat(encoded.toArrayUnsafe()).contains((byte) 0xc0);

    // and the decoder agrees it is an empty list rather than a zero length string
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.POOLED_TRANSACTION_ETH_72);
    assertThat(decoded.getBlobsWithCommitments().orElseThrow().getBlobProofBundles())
        .allSatisfy(bundle -> assertThat(bundle.getBlob()).isEmpty());
  }

  @Test
  void elidedTransactionStartsWithNoCellsAndOneCellSetPerBlob() {
    final Transaction decoded = roundTripElided(blobTransaction());

    final List<BlobProofBundle> bundles =
        decoded.getBlobsWithCommitments().orElseThrow().getBlobProofBundles();

    for (final BlobProofBundle bundle : bundles) {
      assertThat(bundle.getBlob()).isEmpty();
      final CellsWithMask cells = bundle.getCellsWithMask().orElseThrow();
      assertThat(cells.getCellMask().isEmpty()).isTrue();
      assertThat(cells.getCells()).isEmpty();
    }

    // The blobs may share one empty instance, since CellsWithMask is a value: what must hold is
    // that gathering cells for one blob leaves the others as they were.
    final CellsWithMask firstBlobCells =
        bundles.getFirst().getCellsWithMask().orElseThrow().merge(oneCell());

    assertThat(firstBlobCells.getCells()).hasSize(1);
    for (final BlobProofBundle bundle : bundles) {
      assertThat(bundle.getCellsWithMask().orElseThrow().getCells()).isEmpty();
    }
  }

  private static CellsWithMask oneCell() {
    return new CellsWithMask(
        List.of(new Cell(Bytes.repeat((byte) 1, Cell.SIZE))),
        CellMask.fromBytes(Bytes.fromHexString("0x01" + "00".repeat(15))));
  }
}
