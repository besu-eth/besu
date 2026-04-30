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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArchiveIndexReaderTest {

  private SegmentedKeyValueStorage storage;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
  }

  // ── isIndexComplete ───────────────────────────────────────────────────────

  @Test
  public void isIndexComplete_returnsFalseWhenNoLastIndexedBlock() {
    assertThat(ArchiveIndexReader.isIndexComplete(storage, 100L)).isFalse();
  }

  @Test
  public void isIndexComplete_returnsTrueWhenLastIndexedBlockCoversTarget() {
    advanceLastIndexed(0L);
    advanceLastIndexed(1L);
    advanceLastIndexed(2L);

    assertThat(ArchiveIndexReader.isIndexComplete(storage, 0L)).isTrue();
    assertThat(ArchiveIndexReader.isIndexComplete(storage, 1L)).isTrue();
    assertThat(ArchiveIndexReader.isIndexComplete(storage, 2L)).isTrue();
  }

  @Test
  public void isIndexComplete_returnsFalseWhenTargetBeyondLastIndexedBlock() {
    advanceLastIndexed(0L);
    advanceLastIndexed(1L);

    assertThat(ArchiveIndexReader.isIndexComplete(storage, 2L)).isFalse();
    assertThat(ArchiveIndexReader.isIndexComplete(storage, 1000L)).isFalse();
  }

  @Test
  public void isIndexComplete_returnsTrueExactlyAtLastIndexedBlock() {
    advanceLastIndexed(0L);
    advanceLastIndexed(1L);
    advanceLastIndexed(2L);
    advanceLastIndexed(3L);

    assertThat(ArchiveIndexReader.isIndexComplete(storage, 3L)).isTrue();
    assertThat(ArchiveIndexReader.isIndexComplete(storage, 4L)).isFalse();
  }

  // ── findExactAccountBlock ─────────────────────────────────────────────────

  @Test
  public void findExactAccountBlock_returnsEmptyWhenNoIndexEntry() {
    final byte[] accountHash = accountHash(1);

    final java.util.Optional<Long> result =
        ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 500L);

    assertThat(result).isEmpty();
  }

  @Test
  public void findExactAccountBlock_returnsExactMatchWhenPresent() {
    final byte[] accountHash = accountHash(2);
    appendAccount(accountHash, 100L);
    appendAccount(accountHash, 200L);
    appendAccount(accountHash, 300L);

    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 100L)).contains(100L);
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 200L)).contains(200L);
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 300L)).contains(300L);
  }

  @Test
  public void findExactAccountBlock_returnsNearestPredecessorWhenNoExactMatch() {
    final byte[] accountHash = accountHash(3);
    appendAccount(accountHash, 100L);
    appendAccount(accountHash, 300L);
    appendAccount(accountHash, 500L);

    // 250 is between 100 and 300; nearest before is 100
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 250L)).contains(100L);

    // 400 is between 300 and 500; nearest before is 300
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 400L)).contains(300L);

    // 600 is beyond the last entry; nearest before is 500
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 600L)).contains(500L);
  }

  @Test
  public void findExactAccountBlock_returnsEmptyWhenTargetBeforeAllEntries() {
    final byte[] accountHash = accountHash(4);
    appendAccount(accountHash, 200L);
    appendAccount(accountHash, 400L);

    // Target is before every recorded block in this range
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 50L)).isEmpty();
  }

  @Test
  public void findExactAccountBlock_singleEntryExactMatch() {
    final byte[] accountHash = accountHash(5);
    appendAccount(accountHash, 77L);

    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 77L)).contains(77L);
  }

  @Test
  public void findExactAccountBlock_singleEntryNearestBefore() {
    final byte[] accountHash = accountHash(6);
    appendAccount(accountHash, 77L);

    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 99L)).contains(77L);
  }

  @Test
  public void findExactAccountBlock_singleEntryTargetBefore() {
    final byte[] accountHash = accountHash(7);
    appendAccount(accountHash, 77L);

    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 50L)).isEmpty();
  }

  @Test
  public void findExactAccountBlock_doesNotReturnEntryFromDifferentAccount() {
    final byte[] accountHashA = accountHash(8);
    final byte[] accountHashB = accountHash(9);
    appendAccount(accountHashA, 100L);

    // B has no entries; should return empty even though A has entries in the same range
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHashB, 100L)).isEmpty();
  }

  @Test
  public void findExactAccountBlock_manyEntriesBinarySearchCorrect() {
    final byte[] accountHash = accountHash(10);
    // Write 1000 entries: blocks 1, 2, 3, ..., 1000 (all in range 0)
    for (long b = 1; b <= 1000L; b++) {
      appendAccount(accountHash, b);
    }

    // Exact matches at boundaries
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 1L)).contains(1L);
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 1000L))
        .contains(1000L);

    // Mid-range exact match
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 500L)).contains(500L);

    // Target beyond last entry in range (still in same range bucket)
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 1500L))
        .contains(1000L);
  }

  // ── findExactStorageBlock ─────────────────────────────────────────────────

  @Test
  public void findExactStorageBlock_returnsEmptyWhenNoIndexEntry() {
    final byte[] naturalKey = naturalKey(1, 2);

    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKey, 100L)).isEmpty();
  }

  @Test
  public void findExactStorageBlock_returnsExactMatch() {
    final byte[] naturalKey = naturalKey(3, 4);
    appendStorage(naturalKey, 50L);
    appendStorage(naturalKey, 150L);

    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKey, 50L)).contains(50L);
    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKey, 150L)).contains(150L);
  }

  @Test
  public void findExactStorageBlock_returnsNearestPredecessor() {
    final byte[] naturalKey = naturalKey(5, 6);
    appendStorage(naturalKey, 100L);
    appendStorage(naturalKey, 300L);

    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKey, 200L)).contains(100L);
  }

  @Test
  public void findExactStorageBlock_returnsEmptyWhenTargetBeforeAllEntries() {
    final byte[] naturalKey = naturalKey(7, 8);
    appendStorage(naturalKey, 500L);

    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKey, 10L)).isEmpty();
  }

  @Test
  public void findExactStorageBlock_doesNotReturnEntryFromDifferentSlot() {
    final byte[] naturalKeyA = naturalKey(9, 10);
    final byte[] naturalKeyB = naturalKey(9, 11); // same account, different slot
    appendStorage(naturalKeyA, 200L);

    assertThat(ArchiveIndexReader.findExactStorageBlock(storage, naturalKeyB, 200L)).isEmpty();
  }

  // ── cross-range boundary ─────────────────────────────────────────────────

  @Test
  public void findExactAccountBlock_returnsEmptyForQueryInUnindexedRange() {
    final byte[] accountHash = accountHash(20);
    // Write entries only in range 0 (block < 100_000)
    appendAccount(accountHash, 50_000L);
    appendAccount(accountHash, 99_999L);

    // Query in range 1 (100_000 <= block < 200_000) — no index entry exists there
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 150_000L)).isEmpty();
  }

  @Test
  public void findExactAccountBlock_returnsCorrectEntryPerRange() {
    final byte[] accountHash = accountHash(21);
    // Range 0: block 50_000
    appendAccount(accountHash, 50_000L);
    // Range 1: block 150_000
    appendAccount(accountHash, 150_000L);

    // Query in range 0
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 75_000L))
        .contains(50_000L);

    // Query in range 1
    assertThat(ArchiveIndexReader.findExactAccountBlock(storage, accountHash, 175_000L))
        .contains(150_000L);
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  /** Advance lastIndexedBlock contiguously from whatever the current value is. */
  private void advanceLastIndexed(final long block) {
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, block);
  }

  private void appendAccount(final byte[] accountHash, final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendAccountToIndex(storage, tx, accountHash, blockNumber);
    tx.commit();
  }

  private void appendStorage(final byte[] naturalKey, final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendStorageToIndex(storage, tx, naturalKey, blockNumber);
    tx.commit();
  }

  private static byte[] accountHash(final int seed) {
    return Address.fromHexString(String.format("0x%040x", seed))
        .addressHash()
        .getBytes()
        .toArrayUnsafe();
  }

  private static byte[] naturalKey(final int accountSeed, final int slotSeed) {
    final Hash accountHash =
        Address.fromHexString(String.format("0x%040x", accountSeed)).addressHash();
    final Hash slotHash = Address.fromHexString(String.format("0x%040x", slotSeed)).addressHash();
    return Bytes.concatenate(accountHash.getBytes(), slotHash.getBytes()).toArrayUnsafe();
  }
}
