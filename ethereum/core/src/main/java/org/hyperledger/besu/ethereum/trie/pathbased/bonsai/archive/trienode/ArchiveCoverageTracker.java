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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Tracks how much of the chain's trie-node history has been indexed into the archive, and answers
 * whether a given block is covered by that index.
 *
 * <p>Coverage is stored directly in {@code TRIE_BRANCH_STORAGE_ARCHIVE} as two big-endian longs,
 * {@code startBlock} and {@code lastBlock}, followed by the 32-byte hash of {@code lastBlock}.
 * Coverage always grows contiguously from block 0, one block at a time. There is no in-memory
 * state; every read and every {@link #record} write goes straight to storage.
 */
public final class ArchiveCoverageTracker {

  /** The contiguous block range {@code [startBlock, lastBlock]} that has been archived. */
  public record Coverage(long startBlock, long lastBlock, Hash lastBlockHash) {}

  @VisibleForTesting
  public static final byte[] COVERAGE_KEY =
      "ARCHIVE_TRIE_COVERAGE_KEY".getBytes(StandardCharsets.UTF_8);

  private final SegmentedKeyValueStorage storage;

  public ArchiveCoverageTracker(final SegmentedKeyValueStorage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  /** Reads the committed coverage from storage; empty if none or the stored value is malformed. */
  public Optional<Coverage> getCoverage() {
    return storage
        .get(TRIE_BRANCH_STORAGE_ARCHIVE, COVERAGE_KEY)
        .filter(raw -> raw.length == 48)
        .map(
            raw -> {
              final Bytes b = Bytes.wrap(raw);
              return new Coverage(
                  b.getLong(0), b.getLong(8), Hash.wrap(Bytes32.wrap(b.slice(16, 32))));
            });
  }

  /**
   * Returns {@code true} if {@code block} falls within the contiguous range of blocks that have
   * been indexed into the archive.
   */
  public boolean hasArchiveBlock(final long block) {
    return getCoverage().map(c -> block >= c.startBlock() && block <= c.lastBlock()).orElse(false);
  }

  /**
   * Writes updated progress for {@code block}, with hash {@code blockHash}, into {@code tx}. The
   * write is atomic with the archive-node writes that precede it in the same transaction.
   *
   * @throws IllegalStateException if recording {@code block} would leave a gap: it must be {@code
   *     0} when there is no coverage, otherwise {@code lastBlock + 1}
   */
  public void record(
      final SegmentedKeyValueStorageTransaction tx, final long block, final Hash blockHash) {
    final Optional<Coverage> coverage = getCoverage();
    final long expected = coverage.map(c -> c.lastBlock() + 1).orElse(0L);
    if (block != expected) {
      throw new IllegalStateException(
          "Archive coverage must be contiguous: expected block " + expected + " but got " + block);
    }
    final long startBlock = coverage.map(Coverage::startBlock).orElse(block);
    tx.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        COVERAGE_KEY,
        Bytes.concatenate(
                Bytes.ofUnsignedLong(startBlock), Bytes.ofUnsignedLong(block), blockHash.getBytes())
            .toArrayUnsafe());
  }
}
