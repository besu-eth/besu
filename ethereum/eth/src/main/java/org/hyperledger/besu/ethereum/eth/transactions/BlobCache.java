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
package org.hyperledger.besu.ethereum.eth.transactions;

import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.kzg.BlobProofBundle;
import org.hyperledger.besu.ethereum.core.kzg.BlobsWithCommitments;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobCache {
  private final Cache<VersionedHash, BlobProofBundle> cache;
  private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

  public BlobCache() {
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(
                9 * 32 * 3L) // 9 blobs max (since Prague EIP-7691) per 32 slots per 3 epochs
            .expireAfterWrite(
                3 * 32 * 12L, TimeUnit.SECONDS) // 3 epochs of 32 slots which take 12 seconds each.
            .build();
  }

  public void cacheBlobs(final Transaction t) {
    if (t.getType().supportsBlob()) {
      var bwc = t.getBlobsWithCommitments();
      if (bwc.isPresent()) {
        bwc.get().getBlobProofBundles().stream()
            .forEach(
                blobProofBundle ->
                    this.cache.put(blobProofBundle.getVersionedHash(), blobProofBundle));
      } else {
        LOG.debug("transaction is missing blobs, cannot cache");
      }
    }
  }

  public Optional<Transaction> restoreBlob(final Transaction transaction) {
    if (transaction.getType().supportsBlob()) {
      Optional<List<VersionedHash>> maybeHashes = transaction.getVersionedHashes();
      if (maybeHashes.isPresent()) {
        if (!maybeHashes.get().isEmpty()) {
          final List<BlobProofBundle> blobProofBundles =
              maybeHashes.get().stream().map(cache::getIfPresent).toList();

          // A miss leaves a null in the list, so it has to be found before the bundles are used
          // for anything: the entries are keyed by the hash they were looked up with, so a
          // non-null one always matches, and a null one is the only way a restore can fail.
          if (blobProofBundles.stream().anyMatch(Objects::isNull)) {
            LOG.debug("did not find all versioned hashes to restore from cache");
            return Optional.empty();
          }

          return Optional.of(
              Transaction.builder()
                  .copiedFrom(transaction)
                  .blobsWithCommitments(new BlobsWithCommitments(blobProofBundles))
                  .build());
        } else {
          LOG.warn("can't restore blobs for transaction with empty list of versioned hashes");
          return Optional.empty();
        }
      } else {
        LOG.warn("can't restore blobs for transaction without list of versioned hashes");
        return Optional.empty();
      }

    } else {
      LOG.debug(
          "can't restore blobs for non-blob transaction of type {}", transaction.getType().name());
      return Optional.empty();
    }
  }

  public BlobProofBundle get(final VersionedHash vh) {
    return cache.getIfPresent(vh);
  }

  public long size() {
    return cache.estimatedSize();
  }
}
