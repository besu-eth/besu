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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Tracks the contiguous window of blocks {@code [indexStartBlock, lastIndexedBlock]} for which
 * trie-node history is known-complete, so the read path never serves a proof for a block outside
 * that window.
 *
 * <p><strong>Thread-safety:</strong> fields are {@code volatile} so the block-import (writer)
 * thread's advance is visible to proof-serving (reader) threads without external locking. This is
 * deliberate: do not "fix" this into a synchronized/locked design — that would be a regression on
 * the block-import hot path.
 */
public final class TrieNodeHistoryProgress {

  public static final long UNSET_LAST_INDEXED = -1L;
  public static final long UNSET_INDEX_START = Long.MAX_VALUE;

  static final byte[] PROGRESS_KEY = "trieNodeHistoryProgress".getBytes(StandardCharsets.UTF_8);

  private volatile long lastIndexedBlock;
  private volatile long indexStartBlock;

  public TrieNodeHistoryProgress() {
    this(UNSET_LAST_INDEXED, UNSET_INDEX_START);
  }

  private TrieNodeHistoryProgress(final long lastIndexedBlock, final long indexStartBlock) {
    this.lastIndexedBlock = lastIndexedBlock;
    this.indexStartBlock = indexStartBlock;
  }

  public boolean covers(final long block) {
    if (block < 0 || indexStartBlock == UNSET_INDEX_START) {
      return false;
    }
    return block >= indexStartBlock && block <= lastIndexedBlock;
  }

  public long lastIndexedBlock() {
    return lastIndexedBlock;
  }

  public void setLastIndexedBlock(final long n) {
    if (n > lastIndexedBlock) {
      lastIndexedBlock = n;
    }
  }

  public long indexStartBlock() {
    return indexStartBlock;
  }

  public void setIndexStartBlock(final long n) {
    if (n < indexStartBlock) {
      indexStartBlock = n;
    }
  }

  public byte[] toBytes() {
    final ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(lastIndexedBlock);
    buf.putLong(indexStartBlock);
    return buf.array();
  }

  public static TrieNodeHistoryProgress fromBytes(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final long last = buf.getLong();
    final long start = buf.getLong();
    return new TrieNodeHistoryProgress(last, start);
  }

  public static TrieNodeHistoryProgress load(final SegmentedKeyValueStorage storage) {
    return storage
        .get(TRIE_BRANCH_STORAGE, PROGRESS_KEY)
        .map(TrieNodeHistoryProgress::fromBytes)
        .orElseGet(TrieNodeHistoryProgress::new);
  }

  public void save(final SegmentedKeyValueStorageTransaction tx) {
    tx.put(TRIE_BRANCH_STORAGE, PROGRESS_KEY, toBytes());
  }
}
