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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.util.TrustedSetupClassLoaderExtension;

import java.math.BigInteger;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * A transaction whose sidecar holds cells rather than blobs, which is what a node has after
 * receiving one over eth/72 and before it has sampled every cell.
 */
class CellsOnlySidecarTest extends TrustedSetupClassLoaderExtension {

  private static final KeyPair SENDER_KEYS =
      SignatureAlgorithmFactory.getInstance().generateKeyPair();

  /** Cell indexes 64 to 127, a custody set that does not start at zero. */
  private static final CellMask UPPER_HALF =
      CellMask.fromBytes(
          Bytes.concatenate(Bytes.repeat((byte) 0x00, 8), Bytes.repeat((byte) 0xFF, 8)));

  @Test
  void detachedCopyKeepsTheCellsOfASidecarThatHasNoBlobs() {
    // Rebuilding the sidecar from getBlobs() used to be how a transaction was detached, which for a
    // cells-only sidecar reads a list of nulls. detachedCopy is called on the txpool add path, so
    // this is the first thing an eth/72 transaction would have hit.
    final Transaction transaction = cellsOnlyTransaction();

    final Transaction detached = transaction.detachedCopy();

    final BlobsWithCommitments copied = detached.getBlobsWithCommitments().orElseThrow();
    assertThat(copied.hasBlobData()).isFalse();
    assertThat(copied.getCellMask()).isEqualTo(UPPER_HALF);
    assertThat(copied).isEqualTo(transaction.getBlobsWithCommitments().orElseThrow());
    // detached: equal cells, but not the same byte arrays
    final Cell original =
        transaction
            .getBlobsWithCommitments()
            .orElseThrow()
            .getBlobProofBundles()
            .getFirst()
            .getCellsWithMask()
            .orElseThrow()
            .getCell(64);
    final Cell copy =
        copied.getBlobProofBundles().getFirst().getCellsWithMask().orElseThrow().getCell(64);
    assertThat(copy).isEqualTo(original);
    assertThat(copy.getData()).isNotSameAs(original.getData());
  }

  @Test
  void detachedCopyLeavesTheSidecarSharingTheTransactionVersionedHashes() {
    // The sidecar and the transaction hold the same hashes, and detaching should not turn one set
    // of objects into two: the blobpool accounts for the retained size of what it keeps.
    final Transaction detached = cellsOnlyTransaction().detachedCopy();

    assertThat(detached.getBlobsWithCommitments().orElseThrow().getVersionedHashes().getFirst())
        .isSameAs(detached.getVersionedHashes().orElseThrow().getFirst());
  }

  @Test
  void canBeLogged() {
    // Both log lines counted the blobs through getBlobs(), which is null for a sidecar holding
    // cells. Logging a transaction must not be the thing that throws.
    final Transaction transaction = cellsOnlyTransaction();

    assertThat(transaction.toString()).contains("numberOfBlobs=1");
    assertThat(transaction.toTraceLog()).contains("b: 1");
  }

  @Test
  void pooledEncodingOfASidecarThatHasNoBlobsIsRejected() {
    // The eth/68 pooled form carries the blobs themselves, so a transaction holding only cells
    // cannot be encoded into it. The blob list is the right length but holds nothing, so an empty
    // check does not catch this.
    final Transaction transaction = cellsOnlyTransaction();

    assertThatThrownBy(
            () ->
                TransactionEncoder.encodeOpaqueBytes(
                    transaction, EncodingContext.POOLED_TRANSACTION))
        .isInstanceOf(InvalidParameterException.class)
        .hasMessageContaining("cannot be encoded for Pooled Transaction");
  }

  /** A blob transaction whose single sidecar holds only the cells of {@link #UPPER_HALF}. */
  private Transaction cellsOnlyTransaction() {
    final BlobProofBundle full =
        new BlobTestFixture().createBlobProofBundle(BlobType.KZG_CELL_PROOFS);
    final CellsWithMask allCells = full.getCellsWithMask().orElseThrow();
    final List<Cell> held = UPPER_HALF.streamIndexes().mapToObj(allCells::getCell).toList();

    final BlobsWithCommitments cellsOnly =
        BlobsWithCommitments.createFromBlobCells(
            List.of(full.getKzgCommitment()),
            List.of(new CellsWithMask(held, UPPER_HALF)),
            full.getKzgProof(),
            List.of(full.getVersionedHash()));
    assertThat(full.getKzgProof()).hasSize(CKZG4844Helper.CELL_PROOFS_PER_BLOB);

    return new TransactionTestFixture()
        .to(Optional.of(Address.fromHexString("0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF")))
        .type(TransactionType.BLOB)
        .chainId(Optional.of(BigInteger.ONE))
        .maxFeePerGas(Optional.of(Wei.of(15)))
        .maxFeePerBlobGas(Optional.of(Wei.of(7)))
        .maxPriorityFeePerGas(Optional.of(Wei.of(1)))
        .blobsWithCommitments(Optional.of(cellsOnly))
        .versionedHashes(Optional.of(cellsOnly.getVersionedHashes()))
        .createTransaction(SENDER_KEYS);
  }
}
