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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;

import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;

/**
 * Tracks how much of the chain's trie-node history has been indexed into the archive, and answers
 * whether a given block is covered by that index.
 *
 * <p>Coverage is stored directly in {@code TRIE_BRANCH_STORAGE_ARCHIVE} as two consecutive
 * big-endian longs: {@code indexStartBlock} followed by {@code lastIndexedBlock}. There is no
 * in-memory state; every {@link #hasArchiveBlock} read and every {@link #record} write goes
 * straight to storage.
 */
public final class ArchiveCoverageTracker {

  static final byte[] COVERAGE_KEY = "ARCHIVE_TRIE_COVERAGE_KEY".getBytes(StandardCharsets.UTF_8);

  private final SegmentedKeyValueStorage storage;

  public ArchiveCoverageTracker(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  /**
   * Returns {@code true} if {@code block} falls within the contiguous range of blocks that have
   * been indexed into the archive.
   */
  public boolean hasArchiveBlock(final long block) {
    return storage
        .get(TRIE_BRANCH_STORAGE_ARCHIVE, COVERAGE_KEY)
        .map(
            raw -> {
              if (raw.length < 16) return false;
              final Bytes b = Bytes.wrap(raw);
              return block >= b.getLong(0) && block <= b.getLong(8);
            })
        .orElse(false);
  }

  /**
   * Writes updated progress for {@code block} into {@code tx}. The write is atomic with the
   * archive-node writes that precede it in the same transaction.
   *
   * <p>{@code indexStartBlock} is set to {@code min(existing, block)} so the recorded window only
   * ever grows backwards; {@code lastIndexedBlock} is always set to {@code block}.
   */
  public void record(final SegmentedKeyValueStorageTransaction tx, final long block) {
    final long startBlock =
        storage
            .get(TRIE_BRANCH_STORAGE_ARCHIVE, COVERAGE_KEY)
            .map(
                raw -> {
                  final Bytes b = Bytes.wrap(raw);
                  final long prevEnd = b.getLong(8);
                  // Non-contiguous write means the gate was closed for some blocks; start a new
                  // range to avoid claiming coverage over blocks that were never captured.
                  return (block <= prevEnd + 1) ? Math.min(b.getLong(0), block) : block;
                })
            .orElse(block);
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        COVERAGE_KEY,
        Bytes.concatenate(Bytes.ofUnsignedLong(startBlock), Bytes.ofUnsignedLong(block))
            .toArrayUnsafe());
  }
}
