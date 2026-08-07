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
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BonsaiTrieNodeStrategyTest {

  private SegmentedKeyValueStorage storage;
  private BonsaiTrieNodeStrategy strategy;

  @BeforeEach
  void setUp() {
    storage = new SegmentedInMemoryKeyValueStorage();
    strategy = new BonsaiTrieNodeStrategy();
  }

  private void put(final Bytes location, final Bytes node) {
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatAccountTrieNode(storage, tx, location, Bytes32.leftPad(Bytes.EMPTY), node);
    tx.commit();
  }

  @Test
  void putThenGetAccountTrieNodeRoundTrips() {
    final Bytes location = Bytes.fromHexString("0x0102");
    final Bytes node = Bytes.fromHexString("0xdeadbeef");
    put(location, node);
    assertThat(strategy.getFlatAccountTrieNode(location, Bytes32.leftPad(Bytes.EMPTY), storage))
        .contains(node);
  }

  @Test
  void putThenGetStorageTrieNodeRoundTripsKeyedByAccountHashConcatLocation() {
    final Hash accountHash = Hash.hash(Bytes.fromHexString("0xaa"));
    final Bytes location = Bytes.fromHexString("0x0304");
    final Bytes node = Bytes.fromHexString("0xcafebabe");
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putFlatStorageTrieNode(
        storage, tx, accountHash, location, Bytes32.leftPad(Bytes.EMPTY), node);
    tx.commit();

    assertThat(
            strategy.getFlatStorageTrieNode(
                accountHash, location, Bytes32.leftPad(Bytes.EMPTY), storage))
        .contains(node);
    // Confirms the key layout directly, independent of the strategy's own get method.
    assertThat(
            storage.get(
                TRIE_BRANCH_STORAGE,
                Bytes.concatenate(accountHash.getBytes(), location).toArrayUnsafe()))
        .contains(node.toArrayUnsafe());
  }

  @Test
  void removeThenGetReturnsEmpty() {
    final Bytes location = Bytes.fromHexString("0x05");
    put(location, Bytes.fromHexString("0xaa"));
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.removeFlatAccountStateTrieNode(storage, tx, location);
    tx.commit();
    assertThat(strategy.getFlatAccountTrieNode(location, Bytes32.leftPad(Bytes.EMPTY), storage))
        .isEmpty();
  }

  @Test
  void constructingWithNonDefaultSegmentTargetsThatSegmentInstead() {
    // Needed so a future migration design can reuse this class against a different CF without
    // duplicating it — verified here so that guarantee doesn't silently regress.
    final SegmentedKeyValueStorage altStorage = new SegmentedInMemoryKeyValueStorage();
    final BonsaiTrieNodeStrategy altStrategy =
        new BonsaiTrieNodeStrategy(
            org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
                .TRIE_BRANCH_STORAGE_ARCHIVE);
    final Bytes location = Bytes.fromHexString("0x06");
    final Bytes node = Bytes.fromHexString("0xbb");
    final SegmentedKeyValueStorageTransaction tx = altStorage.startTransaction();
    altStrategy.putFlatAccountTrieNode(
        altStorage, tx, location, Bytes32.leftPad(Bytes.EMPTY), node);
    tx.commit();
    assertThat(altStorage.get(TRIE_BRANCH_STORAGE, location.toArrayUnsafe())).isEmpty();
    assertThat(
            altStrategy.getFlatAccountTrieNode(location, Bytes32.leftPad(Bytes.EMPTY), altStorage))
        .contains(node);
  }
}
