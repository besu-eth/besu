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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_FREEZER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_FREEZER;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;
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
import org.bouncycastle.util.Arrays;
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

  @Test
  public void clearAll_removesDataFromAccountInfoStateFreezer() {
    byte[] accountKey =
        Hash.fromHexString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
            .getBytes()
            .toArrayUnsafe();
    byte[] accountValue = Bytes.fromHexString("0xAABBCCDD").toArrayUnsafe();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE_FREEZER, accountKey, accountValue);
    tx.commit();

    assertThat(storage.get(ACCOUNT_INFO_STATE_FREEZER, accountKey)).isNotEmpty();

    archiveFlatDbStrategy.clearAll(storage);

    assertThat(storage.get(ACCOUNT_INFO_STATE_FREEZER, accountKey)).isEmpty();
  }

  @Test
  public void clearAll_removesDataFromAccountStorageFreezer() {
    byte[] storageKey =
        Arrays.concatenate(
            Hash.fromHexString("0x1111111111111111111111111111111111111111111111111111111111111111")
                .getBytes()
                .toArrayUnsafe(),
            Hash.fromHexString("0x2222222222222222222222222222222222222222222222222222222222222222")
                .getBytes()
                .toArrayUnsafe());
    byte[] storageValue = Bytes.fromHexString("0xdeadbeef").toArrayUnsafe();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_STORAGE_FREEZER, storageKey, storageValue);
    tx.commit();

    assertThat(storage.get(ACCOUNT_STORAGE_FREEZER, storageKey)).isNotEmpty();

    archiveFlatDbStrategy.clearAll(storage);

    assertThat(storage.get(ACCOUNT_STORAGE_FREEZER, storageKey)).isEmpty();
  }

  @Test
  public void resetOnResync_removesDataFromAccountInfoStateFreezer() {
    byte[] accountKey =
        Hash.fromHexString("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
            .getBytes()
            .toArrayUnsafe();
    byte[] accountValue = Bytes.fromHexString("0x11223344").toArrayUnsafe();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_INFO_STATE_FREEZER, accountKey, accountValue);
    tx.commit();

    assertThat(storage.get(ACCOUNT_INFO_STATE_FREEZER, accountKey)).isNotEmpty();

    archiveFlatDbStrategy.resetOnResync(storage);

    assertThat(storage.get(ACCOUNT_INFO_STATE_FREEZER, accountKey)).isEmpty();
  }

  @Test
  public void resetOnResync_removesDataFromAccountStorageFreezer() {
    byte[] storageKey =
        Arrays.concatenate(
            Hash.fromHexString("0x3333333333333333333333333333333333333333333333333333333333333333")
                .getBytes()
                .toArrayUnsafe(),
            Hash.fromHexString("0x4444444444444444444444444444444444444444444444444444444444444444")
                .getBytes()
                .toArrayUnsafe());
    byte[] storageValue = Bytes.fromHexString("0xcafebabe").toArrayUnsafe();
    SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(ACCOUNT_STORAGE_FREEZER, storageKey, storageValue);
    tx.commit();

    assertThat(storage.get(ACCOUNT_STORAGE_FREEZER, storageKey)).isNotEmpty();

    archiveFlatDbStrategy.resetOnResync(storage);

    assertThat(storage.get(ACCOUNT_STORAGE_FREEZER, storageKey)).isEmpty();
  }

  private void setWorldBlockNumber(final long blockNumber) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    tx.put(
        TRIE_BRANCH_STORAGE,
        WORLD_BLOCK_NUMBER_KEY,
        Bytes.ofUnsignedLong(blockNumber).toArrayUnsafe());
    tx.commit();
  }
}
