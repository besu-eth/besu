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
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.ethereum.eth.transactions.layered.BaseTransactionPoolTest;

import java.util.Optional;

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
