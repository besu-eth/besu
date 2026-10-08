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
package org.hyperledger.besu.ethereum.eth.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.BlobType;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;
import org.hyperledger.besu.ethereum.core.kzg.CellMask;
import org.hyperledger.besu.ethereum.core.kzg.CellsWithMask;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseTransactionPoolTest;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.junit.jupiter.api.Test;

/** Stripping a blob transaction's sidecar and putting it back from the cache. */
class BlobCacheTest extends BaseTransactionPoolTest {

  private static final KeyPair KEYS = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private final BlobCache blobCache = new BlobCache();

  @Test
  void restoresTheSidecarItCached() {
    final Transaction withBlobs = createEIP4844Transaction(0, KEYS, 1, 2);
    blobCache.cacheBlobs(withBlobs);

    final Optional<Transaction> restored = blobCache.restoreBlob(withoutSidecar(withBlobs));

    assertThat(restored).isPresent();
    assertThat(restored.get().getBlobsWithCommitments())
        .isEqualTo(withBlobs.getBlobsWithCommitments());
    assertThat(restored.get().getHash()).isEqualTo(withBlobs.getHash());
  }

  @Test
  void answersEmptyWhenTheSidecarWasNeverCached() {
    final Transaction withBlobs = createEIP4844Transaction(0, KEYS, 1, 2);

    assertThat(blobCache.restoreBlob(withoutSidecar(withBlobs))).isEmpty();
  }

  @Test
  void answersEmptyWhenOnlySomeOfTheBlobsAreCached() {
    // The cache holds one bundle per versioned hash and expires them individually, so a
    // transaction can easily have some of its blobs evicted and not others. That used to leave a
    // null in the list of looked-up bundles and fail with a NullPointerException instead of
    // reporting that the transaction could not be restored.
    final Transaction withBlobs = createEIP4844Transaction(0, KEYS, 1, 3);
    final Transaction partlyCached = createEIP4844Transaction(0, KEYS, 1, 1);
    blobCache.cacheBlobs(partlyCached);

    assertThat(blobCache.restoreBlob(withoutSidecar(withBlobs))).isEmpty();
  }

  @Test
  void answersEmptyForATransactionThatCarriesNoBlobs() {
    assertThat(blobCache.restoreBlob(createTransaction(0, KEYS))).isEmpty();
  }

  @Test
  void doesNotCacheATransactionHoldingOnlySampledCells() {
    final Transaction withBlobs = createEIP4844CellProofsTransaction(0, 2);
    final Transaction sampled = holdingOnlyCellsOf(withBlobs, withBlobs.getVersionedHashes().get());

    blobCache.cacheBlobs(sampled);

    withBlobs
        .getVersionedHashes()
        .get()
        .forEach(versionedHash -> assertThat(blobCache.get(versionedHash)).isNull());
    assertThat(blobCache.restoreBlob(withoutSidecar(withBlobs))).isEmpty();
  }

  @Test
  void keepsTheFullBlobWhenAnotherTransactionHoldingOnlyCellsOfItIsCachedLater() {
    // The cache keeps one bundle per versioned hash, so a transaction sampling one blob of another
    // used to overwrite that blob alone, and restoring the other then failed on bundles of mixed
    // shapes instead of answering.
    final Transaction withBlobs = createEIP4844CellProofsTransaction(0, 2);
    final VersionedHash sharedBlob = withBlobs.getVersionedHashes().get().getFirst();
    final Transaction samplingOneOfThem = holdingOnlyCellsOf(withBlobs, List.of(sharedBlob));

    blobCache.cacheBlobs(withBlobs);
    blobCache.cacheBlobs(samplingOneOfThem);

    final Optional<Transaction> restored = blobCache.restoreBlob(withoutSidecar(withBlobs));

    assertThat(restored).isPresent();
    assertThat(restored.get().getBlobsWithCommitments())
        .isEqualTo(withBlobs.getBlobsWithCommitments());
  }

  private Transaction createEIP4844CellProofsTransaction(final long nonce, final int blobCount) {
    return createTransaction(
        TransactionType.BLOB,
        nonce,
        Wei.of(5000L),
        Wei.of(500L),
        0,
        blobCount,
        BlobType.KZG_CELL_PROOFS,
        null,
        KEYS);
  }

  /**
   * A transaction carrying the given blobs of {@code withBlobs} as an eth/72 peer would send them:
   * no blob, and only a sample of each blob's cells.
   */
  private static Transaction holdingOnlyCellsOf(
      final Transaction withBlobs, final List<VersionedHash> versionedHashes) {
    final List<BlobProofBundle> bundles =
        withBlobs.getBlobsWithCommitments().orElseThrow().getBlobProofBundles().stream()
            .filter(bundle -> versionedHashes.contains(bundle.getVersionedHash()))
            .toList();
    final CellMask sample = CellMask.FULL.randomSubset(64, new Random(1));
    final BlobsWithCommitments sampled =
        BlobsWithCommitments.createFromBlobCells(
            bundles.stream().map(BlobProofBundle::getKzgCommitment).toList(),
            bundles.stream()
                .map(
                    bundle -> {
                      final CellsWithMask allCells = bundle.getCellsWithMask().orElseThrow();
                      return new CellsWithMask(
                          sample.streamIndexes().mapToObj(allCells::getCell).toList(), sample);
                    })
                .toList(),
            bundles.stream().flatMap(bundle -> bundle.getKzgProof().stream()).toList(),
            versionedHashes);
    assertThat(sampled.hasBlobData()).isFalse();
    return Transaction.builder()
        .copiedFrom(withBlobs)
        .versionedHashes(versionedHashes)
        .blobsWithCommitments(sampled)
        .build();
  }

  /**
   * The transaction as it comes back from a reorged out block, which is the case restoreBlob exists
   * for: a block body carries no sidecar, so decoding one yields the transaction without its blobs.
   */
  private static Transaction withoutSidecar(final Transaction transaction) {
    final Transaction stripped =
        TransactionDecoder.decodeOpaqueBytes(
            TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY),
            EncodingContext.BLOCK_BODY);
    assertThat(stripped.getBlobsWithCommitments()).isEmpty();
    return stripped;
  }
}
