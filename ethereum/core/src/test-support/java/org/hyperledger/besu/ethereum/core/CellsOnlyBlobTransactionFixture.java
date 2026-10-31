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

import static org.hyperledger.besu.ethereum.core.kzg.CKZG4844Helper.CELL_PROOFS_PER_BLOB;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.Cell;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.core.kzg.KZGCommitment;
import org.hyperledger.besu.ethereum.core.kzg.KZGProof;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes48;

/**
 * Builds blob transactions that hold cells and no blob payloads, which is how a node holds a blob
 * transaction received over eth/72.
 *
 * <p>The cells, commitments and proofs are synthetic, so no trusted setup is needed: this fixture
 * suits code paths that move a sidecar around — serving, announcing, encoding — but not ones that
 * verify it cryptographically. Use {@link BlobTestFixture} for those.
 */
public class CellsOnlyBlobTransactionFixture {

  private final KeyPair keys = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private byte byteValue = 0x00;

  /**
   * A transaction holding the cells that {@code cellMask} selects, and no blob payloads.
   *
   * @param blobCount the number of blobs the transaction carries
   * @param cellMask the cells this node holds, the same for every blob as the wire form requires
   * @return the transaction, signed
   */
  public Transaction create(final int blobCount, final CellMask cellMask) {
    final byte seed = byteValue++;
    final List<KZGCommitment> commitments = new ArrayList<>(blobCount);
    final List<KZGProof> proofs = new ArrayList<>(blobCount * CELL_PROOFS_PER_BLOB);
    final List<CellsWithMask> cellsWithMasks = new ArrayList<>(blobCount);
    final List<VersionedHash> versionedHashes = new ArrayList<>(blobCount);

    for (int b = 0; b < blobCount; b++) {
      final List<Cell> cells = new ArrayList<>(cellMask.cardinality());
      cellMask
          .streamIndexes()
          .forEach(index -> cells.add(new Cell(Bytes.repeat((byte) index, Cell.SIZE))));

      final KZGCommitment commitment =
          new KZGCommitment(
              Bytes48.wrap(
                  Bytes.concatenate(Bytes.of(seed, (byte) b), Bytes.repeat((byte) 1, 46))));

      commitments.add(commitment);
      cellsWithMasks.add(new CellsWithMask(cells, cellMask));
      // Proofs are never elided on the wire, so a blob always carries its full set of cell proofs
      // even when only some of its cells are held.
      proofs.addAll(
          Collections.nCopies(
              CELL_PROOFS_PER_BLOB, new KZGProof(Bytes48.wrap(Bytes.repeat((byte) 2, 48)))));
      versionedHashes.add(versionedHashOf(commitment));
    }

    return create(
        BlobsWithCommitments.createFromBlobCells(
            commitments, cellsWithMasks, proofs, versionedHashes));
  }

  /**
   * A transaction carrying the given sidecar, for callers that need genuine KZG material rather
   * than this fixture's synthetic cells.
   *
   * @param blobs the sidecar, which must hold cells rather than blobs
   * @return the transaction, signed
   */
  public Transaction create(final BlobsWithCommitments blobs) {
    final byte seed = byteValue++;
    return Transaction.builder()
        .type(TransactionType.BLOB)
        .chainId(BigInteger.ONE)
        .nonce(seed)
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(Wei.of(10))
        .maxFeePerBlobGas(Wei.of(10))
        .gasLimit(100_000)
        .to(Address.ZERO)
        .value(Wei.ZERO)
        .payload(Bytes.EMPTY)
        .versionedHashes(blobs.getVersionedHashes())
        .blobsWithCommitments(blobs)
        .signAndBuild(keys);
  }

  /**
   * Decoders check each commitment against the versioned hash in the transaction body, so a
   * synthetic commitment still needs its real hash.
   */
  private static VersionedHash versionedHashOf(final KZGCommitment commitment) {
    return new VersionedHash(
        VersionedHash.SHA256_VERSION_ID,
        Hash.wrap(org.hyperledger.besu.crypto.Hash.sha256(commitment.getData())));
  }
}
