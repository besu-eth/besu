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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_MIGRATION;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_BLOCK_NUMBER_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.cache.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Trie node strategy used by the Bonsai archive flat-DB migrator during checkpoint trie rebuilds.
 *
 * <p>Reads from {@code TRIE_BRANCH_MIGRATION} (the frontier CF) via a plain point {@code get()} —
 * bloom-accelerated and cache-resident, identical in performance to a normal bonsai trie build.
 * Falls back to live {@code TRIE_BRANCH_STORAGE} for genesis/unchanged nodes not yet written to the
 * frontier CF.
 *
 * <p>Writes each new/changed node to <em>both</em>:
 *
 * <ol>
 *   <li>{@code TRIE_BRANCH_MIGRATION} — plain key, overwrite-in-place (the frontier read substrate)
 *   <li>{@code TRIE_BRANCH_STORAGE_ARCHIVE} — suffixed by the checkpoint window boundary (proof
 *       output, identical to before)
 * </ol>
 *
 * <p>The proof query path ({@link BonsaiArchiveTrieNodeStrategy}) is untouched — it continues to
 * read from {@code TRIE_BRANCH_STORAGE_ARCHIVE} via {@code getNearestBeforeMatchLength}.
 */
public class BonsaiArchiveMigrationTrieNodeStrategy implements TrieNodeStrategy {

  private final Long trieNodeCheckpointInterval;
  private final BonsaiCachedMerkleTrieLoader trieLoader;
  private final TrieNodeStrategy frontierReadStrategy;
  private final TrieNodeStrategy liveTrieFallback;
  private volatile boolean intervalSeeded = false;

  public BonsaiArchiveMigrationTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    this.trieNodeCheckpointInterval = trieNodeCheckpointInterval;
    this.trieLoader = trieLoader;
    this.frontierReadStrategy = new BonsaiTrieNodeStrategy(TRIE_BRANCH_MIGRATION);
    this.liveTrieFallback = new BonsaiTrieNodeStrategy();
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    return frontierReadStrategy
        .getFlatAccountTrieNode(location, nodeHash, storage)
        .or(() -> liveTrieFallback.getFlatAccountTrieNode(location, nodeHash, storage));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    return frontierReadStrategy
        .getFlatStorageTrieNode(accountHash, location, nodeHash, storage)
        .or(
            () ->
                liveTrieFallback.getFlatStorageTrieNode(accountHash, location, nodeHash, storage));
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    // Frontier CF: plain key, overwrite-in-place
    frontierReadStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    // Archive CF: suffixed for proof serving
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getWriteContext(storage);
      transaction.put(
          TRIE_BRANCH_STORAGE_ARCHIVE,
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(ctx, location.toArrayUnsafe()),
          node.toArrayUnsafe());
    }
    if (trieLoader != null) {
      trieLoader.putAccountNode(nodeHash, node);
    }
  }

  @Override
  public void putFlatStorageTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    // Frontier CF: plain key, overwrite-in-place
    frontierReadStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    // Archive CF: suffixed for proof serving
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getWriteContext(storage);
      final Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
      transaction.put(
          TRIE_BRANCH_STORAGE_ARCHIVE,
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(
              ctx, accountHashLocation.toArrayUnsafe()),
          node.toArrayUnsafe());
    }
    if (trieLoader != null) {
      trieLoader.putStorageNode(nodeHash, node);
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    frontierReadStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  private BonsaiContext getWriteContext(final SegmentedKeyValueStorage storage) {
    final Optional<byte[]> proofBlockNumber =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlockNumber.isPresent()) {
      return new BonsaiContext(Bytes.wrap(proofBlockNumber.get()).toLong());
    }
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(
            bytes -> {
              final long blockNumber = Bytes.wrap(bytes).toLong();
              final long windowStart =
                  ((blockNumber + 1) / trieNodeCheckpointInterval) * trieNodeCheckpointInterval;
              return new BonsaiContext(windowStart);
            })
        .orElse(new BonsaiContext(0L));
  }

  private void ensureIntervalSeeded(final SegmentedKeyValueStorage storage) {
    if (intervalSeeded) return;
    synchronized (this) {
      if (intervalSeeded) return;
      storage
          .get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY)
          .ifPresentOrElse(
              persistedBytes -> {
                final long persisted = Bytes.wrap(persistedBytes).toLong();
                if (persisted != trieNodeCheckpointInterval) {
                  throw new RuntimeException(
                      "Checkpoint interval mismatch (DB="
                          + persisted
                          + ", config="
                          + trieNodeCheckpointInterval
                          + ")");
                }
              },
              () -> {
                final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
                tx.put(
                    TRIE_BRANCH_STORAGE_ARCHIVE,
                    ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY,
                    Bytes.ofUnsignedLong(trieNodeCheckpointInterval).toArrayUnsafe());
                tx.commit();
              });
      intervalSeeded = true;
    }
  }
}
