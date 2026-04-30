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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_ACCOUNT_INDEX;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ARCHIVE_STORAGE_INDEX;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArchiveIndexWriterTest {

  private SegmentedKeyValueStorage storage;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
  }

  // ── appendAccountToIndex ─────────────────────────────────────────────────

  @Test
  public void appendAccountToIndex_writesBlockNumberInCorrectSegment() {
    final Hash accountHash = address(1).addressHash();
    final long blockNumber = 500L;

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendAccountToIndex(
        storage, tx, accountHash.getBytes().toArrayUnsafe(), blockNumber);
    tx.commit();

    final long rangeId = blockNumber / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] indexKey =
        ArchiveIndexWriter.buildAccountIndexKey(accountHash.getBytes().toArrayUnsafe(), rangeId);
    final Optional<byte[]> stored = storage.get(ARCHIVE_ACCOUNT_INDEX, indexKey);

    assertThat(stored).isPresent();
    assertThat(stored.get().length).isEqualTo(Long.BYTES);
    assertThat(Bytes.wrap(stored.get()).toLong()).isEqualTo(blockNumber);
  }

  @Test
  public void appendAccountToIndex_appendsMultipleBlocksInOrder() {
    final Hash accountHash = address(2).addressHash();
    final long block1 = 100L;
    final long block2 = 200L;
    final long block3 = 300L;

    appendAccount(accountHash, block1);
    appendAccount(accountHash, block2);
    appendAccount(accountHash, block3);

    final long rangeId = block1 / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] indexKey =
        ArchiveIndexWriter.buildAccountIndexKey(accountHash.getBytes().toArrayUnsafe(), rangeId);
    final byte[] stored = storage.get(ARCHIVE_ACCOUNT_INDEX, indexKey).get();

    assertThat(stored.length).isEqualTo(3 * Long.BYTES);
    assertThat(Bytes.wrap(stored, 0, Long.BYTES).toLong()).isEqualTo(block1);
    assertThat(Bytes.wrap(stored, Long.BYTES, Long.BYTES).toLong()).isEqualTo(block2);
    assertThat(Bytes.wrap(stored, 2 * Long.BYTES, Long.BYTES).toLong()).isEqualTo(block3);
  }

  @Test
  public void appendAccountToIndex_blocksInDifferentRangesGoToDifferentEntries() {
    final Hash accountHash = address(3).addressHash();
    final long blockInRange0 = 50_000L;
    final long blockInRange1 = 150_000L; // range 1

    appendAccount(accountHash, blockInRange0);
    appendAccount(accountHash, blockInRange1);

    final long rangeId0 = blockInRange0 / ArchiveIndexWriter.RANGE_SIZE;
    final long rangeId1 = blockInRange1 / ArchiveIndexWriter.RANGE_SIZE;
    assertThat(rangeId0).isNotEqualTo(rangeId1);

    final byte[] key0 =
        ArchiveIndexWriter.buildAccountIndexKey(accountHash.getBytes().toArrayUnsafe(), rangeId0);
    final byte[] key1 =
        ArchiveIndexWriter.buildAccountIndexKey(accountHash.getBytes().toArrayUnsafe(), rangeId1);

    assertThat(storage.get(ARCHIVE_ACCOUNT_INDEX, key0)).isPresent();
    assertThat(storage.get(ARCHIVE_ACCOUNT_INDEX, key1)).isPresent();

    assertThat(Bytes.wrap(storage.get(ARCHIVE_ACCOUNT_INDEX, key0).get()).toLong())
        .isEqualTo(blockInRange0);
    assertThat(Bytes.wrap(storage.get(ARCHIVE_ACCOUNT_INDEX, key1).get()).toLong())
        .isEqualTo(blockInRange1);
  }

  @Test
  public void appendAccountToIndex_doesNotWriteToStorageIndexSegment() {
    final Hash accountHash = address(4).addressHash();

    appendAccount(accountHash, 42L);

    final long rangeId = 42L / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] key =
        ArchiveIndexWriter.buildAccountIndexKey(accountHash.getBytes().toArrayUnsafe(), rangeId);

    assertThat(storage.get(ARCHIVE_STORAGE_INDEX, key)).isEmpty();
  }

  // ── appendStorageToIndex ─────────────────────────────────────────────────

  @Test
  public void appendStorageToIndex_writesBlockNumberInCorrectSegment() {
    final byte[] naturalKey = naturalKey(address(5), address(6));
    final long blockNumber = 999L;

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendStorageToIndex(storage, tx, naturalKey, blockNumber);
    tx.commit();

    final long rangeId = blockNumber / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] indexKey = ArchiveIndexWriter.buildStorageIndexKey(naturalKey, rangeId);
    final Optional<byte[]> stored = storage.get(ARCHIVE_STORAGE_INDEX, indexKey);

    assertThat(stored).isPresent();
    assertThat(Bytes.wrap(stored.get()).toLong()).isEqualTo(blockNumber);
  }

  @Test
  public void appendStorageToIndex_appendsMultipleBlocks() {
    final byte[] naturalKey = naturalKey(address(7), address(8));

    appendStorage(naturalKey, 10L);
    appendStorage(naturalKey, 20L);

    final long rangeId = 10L / ArchiveIndexWriter.RANGE_SIZE;
    final byte[] stored =
        storage
            .get(
                ARCHIVE_STORAGE_INDEX, ArchiveIndexWriter.buildStorageIndexKey(naturalKey, rangeId))
            .get();

    assertThat(stored.length).isEqualTo(2 * Long.BYTES);
    assertThat(Bytes.wrap(stored, 0, Long.BYTES).toLong()).isEqualTo(10L);
    assertThat(Bytes.wrap(stored, Long.BYTES, Long.BYTES).toLong()).isEqualTo(20L);
  }

  // ── tryAdvanceLastIndexedBlock ────────────────────────────────────────────

  @Test
  public void tryAdvanceLastIndexedBlock_advancesFromMinusOneToZero() {
    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).isEmpty();

    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 0L);

    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).contains(0L);
  }

  @Test
  public void tryAdvanceLastIndexedBlock_advancesContiguously() {
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 0L);
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 1L);
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 2L);

    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).contains(2L);
  }

  @Test
  public void tryAdvanceLastIndexedBlock_doesNotAdvanceWhenGapExists() {
    // Jump from -1 to 5 (not contiguous from 0)
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 5L);

    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).isEmpty();
  }

  @Test
  public void tryAdvanceLastIndexedBlock_doesNotAdvanceWhenNonContiguous() {
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 0L); // sets to 0
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 2L); // skips 1 — no advance

    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).contains(0L);
  }

  @Test
  public void tryAdvanceLastIndexedBlock_doesNotGoBackward() {
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 0L);
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 1L);
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 2L);
    // Try to set to 1 again — should not move pointer back
    ArchiveIndexWriter.tryAdvanceLastIndexedBlock(storage, 1L);

    assertThat(ArchiveIndexWriter.getLastIndexedBlock(storage)).contains(2L);
  }

  // ── buildAccountIndexKey / buildStorageIndexKey ───────────────────────────

  @Test
  public void buildAccountIndexKey_hasCorrectLength() {
    final byte[] accountHash = address(9).addressHash().getBytes().toArrayUnsafe();
    final byte[] key = ArchiveIndexWriter.buildAccountIndexKey(accountHash, 7L);

    // 32 bytes accountHash + 8 bytes rangeId
    assertThat(key.length).isEqualTo(32 + Long.BYTES);
  }

  @Test
  public void buildStorageIndexKey_hasCorrectLength() {
    final byte[] naturalKey = naturalKey(address(10), address(11));
    final byte[] key = ArchiveIndexWriter.buildStorageIndexKey(naturalKey, 3L);

    // 64 bytes naturalKey + 8 bytes rangeId
    assertThat(key.length).isEqualTo(64 + Long.BYTES);
  }

  @Test
  public void buildAccountIndexKey_rangeIdEncodedBigEndian() {
    final byte[] accountHash = address(12).addressHash().getBytes().toArrayUnsafe();
    final long rangeId = 1L;
    final byte[] key = ArchiveIndexWriter.buildAccountIndexKey(accountHash, rangeId);

    // Last 8 bytes should be big-endian encoding of rangeId
    final Bytes rangeBytes = Bytes.wrap(key, 32, Long.BYTES);
    assertThat(rangeBytes.toLong()).isEqualTo(rangeId);
  }

  @Test
  public void differentAccountsProduceDifferentIndexKeys() {
    final byte[] key1 =
        ArchiveIndexWriter.buildAccountIndexKey(
            address(13).addressHash().getBytes().toArrayUnsafe(), 0L);
    final byte[] key2 =
        ArchiveIndexWriter.buildAccountIndexKey(
            address(14).addressHash().getBytes().toArrayUnsafe(), 0L);

    assertThat(key1).isNotEqualTo(key2);
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private void appendAccount(final Hash accountHash, final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendAccountToIndex(
        storage, tx, accountHash.getBytes().toArrayUnsafe(), blockNumber);
    tx.commit();
  }

  private void appendStorage(final byte[] naturalKey, final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    ArchiveIndexWriter.appendStorageToIndex(storage, tx, naturalKey, blockNumber);
    tx.commit();
  }

  private static Address address(final int seed) {
    return Address.fromHexString(String.format("0x%040x", seed));
  }

  private static byte[] naturalKey(final Address account, final Address slot) {
    // Use address hash as stand-in for slot hash (just needs 32 + 32 = 64 bytes)
    return Bytes.concatenate(account.addressHash().getBytes(), slot.addressHash().getBytes())
        .toArrayUnsafe();
  }
}
