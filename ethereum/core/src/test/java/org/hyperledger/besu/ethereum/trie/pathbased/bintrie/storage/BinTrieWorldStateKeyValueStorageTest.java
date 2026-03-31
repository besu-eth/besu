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
package org.hyperledger.besu.ethereum.trie.pathbased.bintrie.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.InMemoryKeyValueStorageProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.FlatDbMode;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinTrieWorldStateKeyValueStorageTest {

  private BinTrieWorldStateKeyValueStorage storage;

  @BeforeEach
  void setUp() {
    storage =
        new BinTrieWorldStateKeyValueStorage(
            new InMemoryKeyValueStorageProvider(),
            new NoOpMetricsSystem(),
            DataStorageConfiguration.DEFAULT_BINTRIE_CONFIG);
  }

  private BinTrieWorldStateKeyValueStorage.BinTrieUpdater getUpdater() {
    return (BinTrieWorldStateKeyValueStorage.BinTrieUpdater) storage.updater();
  }

  @Test
  void getCode_returnsEmpty() {
    assertThat(storage.getCode(Hash.EMPTY, Hash.EMPTY)).contains(Bytes.EMPTY);
  }

  @Test
  void getCode_saveAndGetRegularValue() {
    final Bytes code = Bytes.fromHexString("0x123456");
    final Hash accountHash = Hash.hash(Address.ZERO.getBytes());
    getUpdater().putCode(accountHash, Hash.hash(code), code).commit();

    assertThat(storage.getCode(Hash.hash(code), accountHash)).contains(code);
  }

  @Test
  void getStateTrieNode_returnsEmpty() {
    assertThat(storage.getStateTrieNode(Bytes.EMPTY)).isEmpty();
  }

  @Test
  void putAndGetTrieNode() {
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes value = Bytes.fromHexString("0xDEADBEEF");

    storage.putTrieNode(location, value);

    assertThat(storage.getStateTrieNode(location)).contains(value);
  }

  @Test
  void shouldReturnCorrectDataStorageFormat() {
    assertThat(storage.getDataStorageFormat()).isEqualTo(DataStorageFormat.BINTRIE);
  }

  @Test
  void shouldReturnFullFlatDbMode() {
    assertThat(storage.getFlatDbMode()).isEqualTo(FlatDbMode.FULL);
  }

  @Test
  void shouldReturnNonNullFlatDbStrategy() {
    assertThat(storage.getFlatDbStrategy()).isNotNull();
  }

  @Test
  void updater_putAccountInfoState() {
    final Hash accountHash = Hash.hash(Address.ZERO.getBytes());
    final Bytes accountData = Bytes.fromHexString("0x112233");

    getUpdater().putAccountInfoState(accountHash, accountData).commit();
    // Verification: no exception means the data was stored
  }

  @Test
  void updater_removeAccountInfoState() {
    final Hash accountHash = Hash.hash(Address.ZERO.getBytes());
    final Bytes accountData = Bytes.fromHexString("0x112233");

    BinTrieWorldStateKeyValueStorage.BinTrieUpdater updater = getUpdater();
    updater.putAccountInfoState(accountHash, accountData);
    updater.commit();

    updater = getUpdater();
    updater.removeAccountInfoState(accountHash);
    updater.commit();
  }

  @Test
  void updater_putAndRemoveStorageValue() {
    final Hash accountHash = Hash.hash(Address.ZERO.getBytes());
    final Hash slotHash = Hash.hash(Bytes.fromHexString("0x01"));
    final Bytes32 value =
        Bytes32.fromHexString("0x0000000000000000000000000000000000000000000000000000000000000042");

    BinTrieWorldStateKeyValueStorage.BinTrieUpdater updater = getUpdater();
    updater.putStorageValueBySlotHash(accountHash, slotHash, value);
    updater.commit();

    updater = getUpdater();
    updater.removeStorageValueBySlotHash(accountHash, slotHash);
    updater.commit();
  }

  @Test
  void updater_saveWorldState() {
    final Hash blockHash =
        Hash.fromHexString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    final Bytes32 rootHash =
        Bytes32.fromHexString("0xfedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321");

    getUpdater().saveWorldState(blockHash.getBytes(), rootHash, Bytes.EMPTY).commit();

    assertThat(storage.getWorldStateRootHash()).isPresent();
    assertThat(storage.getWorldStateBlockHash()).contains(blockHash);
  }

  @Test
  void clear_clearsTrieLogs() {
    // Clear should work without throwing exceptions
    storage.clear();
    // Verify state is cleared
    assertThat(storage.getWorldStateRootHash()).isEmpty();
    assertThat(storage.getWorldStateBlockHash()).isEmpty();
  }

  @Test
  void shouldCommitAndRollback() {
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes value = Bytes.fromHexString("0xDEADBEEF");

    BinTrieWorldStateKeyValueStorage.BinTrieUpdater updater = getUpdater();
    updater.putTrieNode(location, value);
    updater.rollback();

    assertThat(storage.getStateTrieNode(location)).isEmpty();
  }

  @Test
  void shouldSupportCommitTrieLogOnly() {
    BinTrieWorldStateKeyValueStorage.BinTrieUpdater updater = getUpdater();
    // Should not throw
    updater.commitTrieLogOnly();
  }

  @Test
  void shouldSupportCommitComposedOnly() {
    final Bytes location = Bytes.fromHexString("0x01");
    final Bytes value = Bytes.fromHexString("0xDEADBEEF");

    BinTrieWorldStateKeyValueStorage.BinTrieUpdater updater = getUpdater();
    updater.putTrieNode(location, value);
    updater.commitComposedOnly();

    assertThat(storage.getStateTrieNode(location)).contains(value);
  }

  @Test
  void shouldCheckWorldStateAvailable() {
    final Bytes32 rootHash = Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes());
    final Hash blockHash = Hash.ZERO;

    // Initially no world state root is stored, so it returns false
    assertThat(storage.isWorldStateAvailable(rootHash, blockHash)).isFalse();

    // Save a world state
    getUpdater().saveWorldState(blockHash.getBytes(), rootHash, Bytes.EMPTY).commit();

    // Now the world state should be available
    assertThat(storage.isWorldStateAvailable(rootHash, blockHash)).isTrue();
  }
}
