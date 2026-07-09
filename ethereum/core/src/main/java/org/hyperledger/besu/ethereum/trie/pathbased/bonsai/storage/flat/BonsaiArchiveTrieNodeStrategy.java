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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive trie node strategy. Reads from {@code TRIE_BRANCH_STORAGE_ARCHIVE} using suffix-based
 * nearest-before lookup; writes to both {@code TRIE_BRANCH_STORAGE} (via delegate) and {@code
 * TRIE_BRANCH_STORAGE_ARCHIVE}.
 */
public class BonsaiArchiveTrieNodeStrategy implements TrieNodeStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BonsaiArchiveTrieNodeStrategy.class);

  /**
   * Plain point-lookup strategy used for the "current trie" reads/writes. Defaults to {@link
   * BonsaiTrieNodeStrategy} over TRIE_BRANCH_STORAGE; the migrator subclass supplies one over its
   * migration column family.
   */
  protected final TrieNodeStrategy baseStrategy;

  private final Long trieNodeCheckpointInterval;
  private final BonsaiCachedMerkleTrieLoader trieLoader;
  private volatile boolean intervalSeeded = false;

  public BonsaiArchiveTrieNodeStrategy(final Long trieNodeCheckpointInterval) {
    this(trieNodeCheckpointInterval, null);
  }

  public BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval, final BonsaiCachedMerkleTrieLoader trieLoader) {
    this(trieNodeCheckpointInterval, trieLoader, new BonsaiTrieNodeStrategy());
  }

  protected BonsaiArchiveTrieNodeStrategy(
      final Long trieNodeCheckpointInterval,
      final BonsaiCachedMerkleTrieLoader trieLoader,
      final TrieNodeStrategy baseStrategy) {
    this.trieNodeCheckpointInterval = trieNodeCheckpointInterval;
    this.trieLoader = trieLoader;
    this.baseStrategy = baseStrategy;
  }

  @Override
  public Optional<Bytes> getFlatAccountTrieNode(
      final Bytes location, final Bytes32 nodeHash, final SegmentedKeyValueStorage storage) {
    Bytes keyNearest =
        BonsaiArchiveKeyUtil.calculateArchiveKeyWithMaxSuffix(
            getStateTrieArchiveContextForRead(storage), location.toArrayUnsafe());
    return storage
        .getNearestBeforeMatchLength(TRIE_BRANCH_STORAGE_ARCHIVE, keyNearest)
        .filter(
            found -> found.key().size() == location.size() + BonsaiArchiveKeyUtil.KEY_SUFFIX_LENGTH)
        .filter(found -> location.commonPrefixLength(found.key()) >= location.size())
        .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes)
        .or(() -> baseStrategy.getFlatAccountTrieNode(location, nodeHash, storage));
  }

  @Override
  public Optional<Bytes> getFlatStorageTrieNode(
      final Hash accountHash,
      final Bytes location,
      final Bytes32 nodeHash,
      final SegmentedKeyValueStorage storage) {
    Bytes accountHashLocation = Bytes.concatenate(accountHash.getBytes(), location);
    Bytes keyNearest =
        BonsaiArchiveKeyUtil.calculateArchiveKeyWithMaxSuffix(
            getStateTrieArchiveContextForRead(storage), accountHashLocation.toArrayUnsafe());
    return storage
        .getNearestBeforeMatchLength(TRIE_BRANCH_STORAGE_ARCHIVE, keyNearest)
        .filter(
            found ->
                found.key().size()
                    == accountHash.getBytes().size()
                        + location.size()
                        + BonsaiArchiveKeyUtil.KEY_SUFFIX_LENGTH)
        .filter(
            found ->
                accountHashLocation.commonPrefixLength(found.key()) >= accountHashLocation.size())
        .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes)
        .or(() -> baseStrategy.getFlatStorageTrieNode(accountHash, location, nodeHash, storage));
  }

  @Override
  public void putFlatAccountTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location,
      final Bytes32 nodeHash,
      final Bytes node) {
    baseStrategy.putFlatAccountTrieNode(storage, transaction, location, nodeHash, node);
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getStateTrieArchiveContextForWrite(storage);
      byte[] keySuffixed =
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(ctx, location.toArrayUnsafe());
      transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, keySuffixed, node.toArrayUnsafe());
      if (trieLoader != null) {
        trieLoader.putAccountNode(nodeHash, node);
      }
      LOG.trace(
          "Archive account trie node written: location={} suffix={}",
          location,
          ctx.getBlockNumber().orElse(-1L));
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
    baseStrategy.putFlatStorageTrieNode(
        storage, transaction, accountHash, location, nodeHash, node);
    if (trieNodeCheckpointInterval != null) {
      ensureIntervalSeeded(storage);
      final BonsaiContext ctx = getStateTrieArchiveContextForWrite(storage);
      byte[] keySuffixed =
          BonsaiArchiveKeyUtil.calculateArchiveKeyWithMinSuffix(
              ctx, Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe());
      transaction.put(TRIE_BRANCH_STORAGE_ARCHIVE, keySuffixed, node.toArrayUnsafe());
      if (trieLoader != null) {
        trieLoader.putStorageNode(nodeHash, node);
      }
      LOG.trace(
          "Archive storage trie node written: account={} location={} suffix={}",
          accountHash,
          location,
          ctx.getBlockNumber().orElse(-1L));
    }
  }

  @Override
  public void removeFlatAccountStateTrieNode(
      final SegmentedKeyValueStorage storage,
      final SegmentedKeyValueStorageTransaction transaction,
      final Bytes location) {
    baseStrategy.removeFlatAccountStateTrieNode(storage, transaction, location);
  }

  /**
   * Checkpoint block C's trie nodes are archived at the start of the window ending at C ({@code
   * (C/interval)*interval}). The proof-serving layer pins that window start under {@code
   * ARCHIVE_PROOF_BLOCK_NUMBER_KEY} so every trie-node read of the proof world state resolves
   * within the rolled-from checkpoint's window. Without it, reads fall back to {@code
   * WORLD_BLOCK_NUMBER_KEY} (the target block number), which for a roll-forward target in the
   * window after C resolves to the next checkpoint's nodes wherever they exist, shadowing
   * checkpoint C's trie.
   */
  private Optional<BonsaiContext> getStateTrieArchiveContextForRead(
      final SegmentedKeyValueStorage storage) {
    // The context is constant for the duration of a proof; memoize it per proof (no-op outside a
    // proof scope) so it isn't re-resolved from storage on every trie-node read.
    return BonsaiArchiveReadContext.trieReadContext(
        () -> {
          Optional<byte[]> proofTrieContext =
              storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
          if (proofTrieContext.isPresent()) {
            return Optional.of(new BonsaiContext(Bytes.wrap(proofTrieContext.get()).toLong()));
          }
          return BonsaiArchiveKeyUtil.getStateArchiveContextForRead(storage);
        });
  }

  private BonsaiContext getStateTrieArchiveContextForWrite(final SegmentedKeyValueStorage storage) {
    Optional<byte[]> proofBlockNumber =
        storage.get(TRIE_BRANCH_STORAGE, ARCHIVE_PROOF_BLOCK_NUMBER_KEY);
    if (proofBlockNumber.isPresent()) {
      return new BonsaiContext(Bytes.wrap(proofBlockNumber.get()).toLong());
    }
    return storage
        .get(TRIE_BRANCH_STORAGE, WORLD_BLOCK_NUMBER_KEY)
        .map(
            bytes -> {
              long blockNumber = Bytes.wrap(bytes).toLong();
              long windowStart =
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
                long persisted = Bytes.wrap(persistedBytes).toLong();
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
                SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
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
