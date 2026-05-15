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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_BLOCK_NUMBER_KEY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BonsaiArchiveFlatDbStrategyTest {

  private BonsaiArchiveFlatDbStrategy archiveFlatDbStrategy;
  private SegmentedKeyValueStorage storage;

  @BeforeEach
  public void setup() {
    storage = new SegmentedInMemoryKeyValueStorage();
    archiveFlatDbStrategy =
        new BonsaiArchiveFlatDbStrategy(new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy());
  }

  @Test
  public void genesisBlockUsesZeroSuffixWhenWorldBlockNumberKeyNotSet() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000001").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xAABBCC");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void block1UsesOneSuffixWhenWorldBlockNumberKeyIsZero() {
    setWorldBlockNumber(0);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000002").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0xDDEEFF");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);

    final byte[] genesisKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    assertThat(storage.get(ACCOUNT_INFO_STATE_ARCHIVE, genesisKey)).isEmpty();
  }

  @Test
  public void block2UsesTwoSuffixWhenWorldBlockNumberKeyIsOne() {
    setWorldBlockNumber(1);

    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000003").addressHash();
    final Bytes accountValue = Bytes.fromHexString("0x112233");

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, accountValue);
    tx.commit();

    final byte[] expectedKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(2)).toArrayUnsafe();
    final Optional<byte[]> storedValue = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, expectedKey);

    assertThat(storedValue).isPresent();
    assertThat(Bytes.wrap(storedValue.get())).isEqualTo(accountValue);
  }

  @Test
  public void genesisAndBlock1AccountsDoNotOverwrite() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000004").addressHash();
    final Bytes genesisAccountValue = Bytes.fromHexString("0xAABBCCDDEEFF00");
    final Bytes block1AccountValue = Bytes.fromHexString("0x112233445566FF");

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, genesisAccountValue);
    tx.commit();

    setWorldBlockNumber(0);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, block1AccountValue);
    tx.commit();

    final byte[] genesisKey =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final byte[] block1Key =
        Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(1)).toArrayUnsafe();

    final Optional<byte[]> genesisValue = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, genesisKey);
    final Optional<byte[]> block1Value = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, block1Key);

    assertThat(genesisValue).isPresent();
    assertThat(Bytes.wrap(genesisValue.get())).isEqualTo(genesisAccountValue);

    assertThat(block1Value).isPresent();
    assertThat(Bytes.wrap(block1Value.get())).isEqualTo(block1AccountValue);
  }

  @Test
  public void sequentialBlocksUseIncrementingSuffixes() {
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000005").addressHash();

    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA00"));
    tx.commit();

    setWorldBlockNumber(0);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA01"));
    tx.commit();

    setWorldBlockNumber(1);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA02"));
    tx.commit();

    setWorldBlockNumber(2);

    tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccount(storage, tx, accountHash, Bytes.fromHexString("0xAA03"));
    tx.commit();

    final Bytes[] expectedValues = {
      Bytes.fromHexString("0xAA00"),
      Bytes.fromHexString("0xAA01"),
      Bytes.fromHexString("0xAA02"),
      Bytes.fromHexString("0xAA03")
    };

    for (long blockNum = 0; blockNum <= 3; blockNum++) {
      final byte[] key =
          Bytes.concatenate(accountHash.getBytes(), Bytes.ofUnsignedLong(blockNum)).toArrayUnsafe();
      final Optional<byte[]> value = storage.get(ACCOUNT_INFO_STATE_ARCHIVE, key);
      assertThat(value).as("Block " + blockNum + " should have stored value").isPresent();
      assertThat(Bytes.wrap(value.get())).isEqualTo(expectedValues[(int) blockNum]);
    }
  }

  private void setWorldBlockNumber(final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }

  // ---- Trie-node proofs tests ----

  private static final long INTERVAL = 10L;
  private static final Bytes TRIE_LOCATION = Bytes.fromHexString("0x0102030405");
  private static final Bytes32 NODE_HASH =
      Bytes32.fromHexString("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  private static final Bytes NODE_VALUE = Bytes.fromHexString("0xdeadbeef");

  private BonsaiArchiveFlatDbStrategy newProofsStrategy() {
    return new BonsaiArchiveFlatDbStrategy(
        new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy(), INTERVAL, false);
  }

  private BonsaiArchiveFlatDbStrategy newProofsReadStrategy() {
    return new BonsaiArchiveFlatDbStrategy(
        new NoOpMetricsSystem(), new CodeHashCodeStorageStrategy(), INTERVAL, true);
  }

  /** window = ((blockNumber + 1) / interval) * interval */
  private long windowStart(final long blockNumber) {
    return ((blockNumber + 1) / INTERVAL) * INTERVAL;
  }

  @Test
  public void putFlatAccountTrieNode_proofsDisabled_doesNotWriteToArchive() {
    // archiveFlatDbStrategy is the no-proofs instance from @BeforeEach
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    archiveFlatDbStrategy.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    assertThat(storage.get(TRIE_BRANCH_STORAGE, TRIE_LOCATION.toArrayUnsafe())).isPresent();
    assertThat(storage.stream(TRIE_BRANCH_STORAGE_ARCHIVE)).isEmpty();
  }

  @Test
  public void putFlatAccountTrieNode_proofsEnabled_writesToBothStorages() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    setWorldBlockNumber(0);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    // current-state write
    assertThat(storage.get(TRIE_BRANCH_STORAGE, TRIE_LOCATION.toArrayUnsafe())).isPresent();

    // archive write: key = location + windowStart(0)
    final byte[] archiveKey =
        Bytes.concatenate(TRIE_LOCATION, Bytes.ofUnsignedLong(windowStart(0))).toArrayUnsafe();
    final Optional<byte[]> archived = storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, archiveKey);
    assertThat(archived).isPresent();
    assertThat(Bytes.wrap(archived.get())).isEqualTo(NODE_VALUE);
  }

  @Test
  public void putFlatStorageTrieNode_proofsEnabled_writesToBothStorages() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000010").addressHash();
    setWorldBlockNumber(0);

    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatStorageTrieNode(storage, tx, accountHash, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    // current-state write
    final byte[] naturalKey =
        Bytes.concatenate(accountHash.getBytes(), TRIE_LOCATION).toArrayUnsafe();
    assertThat(storage.get(TRIE_BRANCH_STORAGE, naturalKey)).isPresent();

    // archive write
    final byte[] archiveKey =
        Bytes.concatenate(
                accountHash.getBytes(), TRIE_LOCATION, Bytes.ofUnsignedLong(windowStart(0)))
            .toArrayUnsafe();
    assertThat(storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, archiveKey)).isPresent();
  }

  @Test
  public void putFlatAccountTrieNode_withinSameWindow_archiveKeyIsOverwritten() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    final Bytes firstValue = Bytes.fromHexString("0x1111");
    final Bytes secondValue = Bytes.fromHexString("0x2222");

    // block 0 → window 0
    setWorldBlockNumber(0);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, firstValue);
    tx.commit();

    // block 4 → window still 0 (((4+1)/10)*10 = 0)
    setWorldBlockNumber(4);
    tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, secondValue);
    tx.commit();

    final byte[] archiveKey =
        Bytes.concatenate(TRIE_LOCATION, Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final Optional<byte[]> value = storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, archiveKey);
    assertThat(value).isPresent();
    assertThat(Bytes.wrap(value.get())).isEqualTo(secondValue);

    // exactly one archive entry for this location proves the second write overwrote the first
    assertThat(
            storage.stream(TRIE_BRANCH_STORAGE_ARCHIVE)
                .filter(
                    e ->
                        TRIE_LOCATION.commonPrefixLength(Bytes.wrap(e.getKey()))
                            >= TRIE_LOCATION.size())
                .count())
        .isEqualTo(1L);
  }

  @Test
  public void putFlatAccountTrieNode_crossWindow_createsSeparateArchiveKeys() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    final Bytes val0 = Bytes.fromHexString("0xAAAA");
    final Bytes val10 = Bytes.fromHexString("0xBBBB");

    // block 0 → window 0
    setWorldBlockNumber(0);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, val0);
    tx.commit();

    // block 9 → window 10 (((9+1)/10)*10 = 10)
    setWorldBlockNumber(9);
    tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, val10);
    tx.commit();

    final byte[] key0 = Bytes.concatenate(TRIE_LOCATION, Bytes.ofUnsignedLong(0)).toArrayUnsafe();
    final byte[] key10 = Bytes.concatenate(TRIE_LOCATION, Bytes.ofUnsignedLong(10)).toArrayUnsafe();

    assertThat(storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, key0)).isPresent();
    assertThat(Bytes.wrap(storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, key0).get())).isEqualTo(val0);

    assertThat(storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, key10)).isPresent();
    assertThat(Bytes.wrap(storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, key10).get())).isEqualTo(val10);
  }

  @Test
  public void getFlatAccountTrieNode_readsLatestBefore() {
    final BonsaiArchiveFlatDbStrategy s = newProofsReadStrategy();

    // write at block 0 → window 0
    setWorldBlockNumber(0);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    // read at block 5 (key = location + 5, nearest before is location + 0)
    setWorldBlockNumber(5);
    final Optional<Bytes> result = s.getFlatAccountTrieNode(TRIE_LOCATION, NODE_HASH, storage);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(NODE_VALUE);
  }

  @Test
  public void getFlatAccountTrieNode_differentLocation_returnsEmpty() {
    final BonsaiArchiveFlatDbStrategy s = newProofsReadStrategy();
    final Bytes otherLocation = Bytes.fromHexString("0x0FFFFFFF00");

    setWorldBlockNumber(0);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    setWorldBlockNumber(1);
    final Optional<Bytes> result = s.getFlatAccountTrieNode(otherLocation, NODE_HASH, storage);
    assertThat(result).isEmpty();
  }

  @Test
  public void getFlatStorageTrieNode_readsLatestBefore() {
    final BonsaiArchiveFlatDbStrategy s = newProofsReadStrategy();
    final Hash accountHash =
        Address.fromHexString("0x0000000000000000000000000000000000000020").addressHash();

    setWorldBlockNumber(0);
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatStorageTrieNode(storage, tx, accountHash, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    setWorldBlockNumber(5);
    final Optional<Bytes> result =
        s.getFlatStorageTrieNode(accountHash, TRIE_LOCATION, NODE_HASH, storage);
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(NODE_VALUE);
  }

  @Test
  public void ensureIntervalSeeded_persistsIntervalOnFirstWrite() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    setWorldBlockNumber(0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    final Optional<byte[]> seeded =
        storage.get(TRIE_BRANCH_STORAGE_ARCHIVE, ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY);
    assertThat(seeded).isPresent();
    assertThat(Bytes.wrap(seeded.get()).toLong()).isEqualTo(INTERVAL);
  }

  @Test
  public void ensureIntervalSeeded_throwsOnIntervalMismatch() {
    // pre-populate with interval 5 then try to use interval 10
    final SegmentedKeyValueStorageTransaction setup = storage.startTransaction();
    setup.put(
        TRIE_BRANCH_STORAGE_ARCHIVE,
        ARCHIVE_PROOF_CHECKPOINT_INTERVAL_KEY,
        Bytes.ofUnsignedLong(5).toArrayUnsafe());
    setup.commit();

    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy(); // interval=10
    setWorldBlockNumber(0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();

    assertThatThrownBy(
            () -> s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Checkpoint interval mismatch");
  }

  @Test
  public void clearAll_withProofsEnabled_clearsTrieBranchArchive() {
    final BonsaiArchiveFlatDbStrategy s = newProofsStrategy();
    setWorldBlockNumber(0);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    s.putFlatAccountTrieNode(storage, tx, TRIE_LOCATION, NODE_HASH, NODE_VALUE);
    tx.commit();

    assertThat(storage.stream(TRIE_BRANCH_STORAGE_ARCHIVE)).isNotEmpty();

    s.clearAll(storage);

    assertThat(storage.stream(TRIE_BRANCH_STORAGE_ARCHIVE)).isEmpty();
  }
}
