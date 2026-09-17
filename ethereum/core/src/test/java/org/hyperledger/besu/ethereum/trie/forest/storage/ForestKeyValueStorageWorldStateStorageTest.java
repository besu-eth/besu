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
package org.hyperledger.besu.ethereum.trie.forest.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.forest.storage.ForestWorldStateKeyValueStorage.Updater;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

public class ForestKeyValueStorageWorldStateStorageTest {

  @Test
  public void getCode_returnsEmpty() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    assertThat(storage.getCode(Hash.EMPTY)).contains(Bytes.EMPTY);
  }

  @Test
  public void getAccountStateTrieNode_returnsEmptyNode() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    assertThat(storage.getAccountStateTrieNode(MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @Test
  public void getAccountStorageTrieNode_returnsEmptyNode() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    assertThat(storage.getAccountStorageTrieNode(MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
  }

  @Test
  public void getCode_saveAndGetSpecialValues() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage.updater().putCode(MerkleTrie.EMPTY_TRIE_NODE).putCode(Bytes.EMPTY).commit();

    assertThat(storage.getCode(Hash.EMPTY_TRIE_HASH)).contains(MerkleTrie.EMPTY_TRIE_NODE);

    assertThat(storage.getCode(Hash.EMPTY)).contains(Bytes.EMPTY);
  }

  @Test
  public void getCode_saveAndGetRegularValue() {
    final Bytes bytes = Bytes.fromHexString("0x123456");
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage.updater().putCode(bytes).commit();

    assertThat(storage.getCode(Hash.hash(bytes))).contains(bytes);
  }

  @Test
  public void getAccountStateTrieNode_saveAndGetSpecialValues() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage
        .updater()
        .putAccountStateTrieNode(
            Bytes32.wrap(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE).getBytes()),
            MerkleTrie.EMPTY_TRIE_NODE)
        .putAccountStateTrieNode(Bytes32.wrap(Hash.hash(Bytes.EMPTY).getBytes()), Bytes.EMPTY)
        .commit();

    assertThat(storage.getAccountStateTrieNode(MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
    assertThat(storage.getAccountStateTrieNode(Bytes32.wrap(Hash.EMPTY.getBytes())))
        .contains(Bytes.EMPTY);
  }

  @Test
  public void getAccountStateTrieNode_saveAndGetRegularValue() {
    final Bytes bytes = Bytes.fromHexString("0x123456");
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage
        .updater()
        .putAccountStateTrieNode(Bytes32.wrap(Hash.hash(bytes).getBytes()), bytes)
        .commit();

    assertThat(storage.getAccountStateTrieNode(Bytes32.wrap(Hash.hash(bytes).getBytes())))
        .contains(bytes);
  }

  @Test
  public void getAccountStorageTrieNode_saveAndGetSpecialValues() {
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage
        .updater()
        .putAccountStorageTrieNode(
            Bytes32.wrap(Hash.hash(MerkleTrie.EMPTY_TRIE_NODE).getBytes()),
            MerkleTrie.EMPTY_TRIE_NODE)
        .putAccountStorageTrieNode(Bytes32.wrap(Hash.hash(Bytes.EMPTY).getBytes()), Bytes.EMPTY)
        .commit();

    assertThat(storage.getAccountStorageTrieNode(MerkleTrie.EMPTY_TRIE_NODE_HASH))
        .contains(MerkleTrie.EMPTY_TRIE_NODE);
    assertThat(storage.getAccountStorageTrieNode(Bytes32.wrap(Hash.EMPTY.getBytes())))
        .contains(Bytes.EMPTY);
  }

  @Test
  public void getAccountStorageTrieNode_saveAndGetRegularValue() {
    final Bytes bytes = Bytes.fromHexString("0x123456");
    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    storage
        .updater()
        .putAccountStorageTrieNode(Bytes32.wrap(Hash.hash(bytes).getBytes()), bytes)
        .commit();

    assertThat(storage.getAccountStateTrieNode(Bytes32.wrap(Hash.hash(bytes).getBytes())))
        .contains(bytes);
  }

  @Test
  public void reconcilesNonConflictingUpdaters() {
    final Bytes bytesA = Bytes.fromHexString("0x12");
    final Bytes bytesB = Bytes.fromHexString("0x1234");
    final Bytes bytesC = Bytes.fromHexString("0x123456");

    final ForestWorldStateKeyValueStorage storage = emptyStorage();
    final Updater updaterA = storage.updater();
    final Updater updaterB = storage.updater();

    updaterA.putCode(bytesA);
    updaterB.putCode(bytesA);
    updaterB.putCode(bytesB);
    updaterA.putCode(bytesC);

    updaterA.commit();
    updaterB.commit();

    assertThat(storage.getCode(Hash.hash(bytesA))).contains(bytesA);
    assertThat(storage.getCode(Hash.hash(bytesB))).contains(bytesB);
    assertThat(storage.getCode(Hash.hash(bytesC))).contains(bytesC);
  }

  @Test
  public void isWorldStateAvailable_defaultIsFalse() {
    assertThat(emptyStorage().isWorldStateAvailable(UInt256.valueOf(1))).isFalse();
  }

  @Test
  public void isWorldStateAvailable_emptyTrieStateAlwaysAvailable() {
    assertThat(emptyStorage().isWorldStateAvailable(Bytes32.wrap(Hash.EMPTY_TRIE_HASH.getBytes())))
        .isTrue();
  }

  private ForestWorldStateKeyValueStorage emptyStorage() {
    return new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
  }
}
