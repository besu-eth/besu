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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.trienode;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiTrieNodeStrategyTest {
  private final SegmentedKeyValueStorage storage =
      new SegmentedInMemoryKeyValueStorage(List.of(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE));
  private final BonsaiTrieNodeStrategy strategy = new BonsaiTrieNodeStrategy();

  private static Bytes32 hash(final Bytes value) {
    return Bytes32.wrap(Hash.hash(value).getBytes());
  }

  @Test
  void accountNodeRoundTripsAtBareLocation() {
    final Bytes location = Bytes.of(0x0e);
    final Bytes node = Bytes.of(0xAA, 0xBB);
    final SegmentedKeyValueStorageTransaction tx = storage.startTransaction();
    strategy.putTrieNode(storage, tx, Optional.empty(), location, hash(node), node);
    tx.commit();
    assertThat(strategy.getTrieNode(location, hash(node), storage)).contains(node);
    // on-disk key is the bare location (format-compatible with legacy bonsai)
    assertThat(storage.get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, location.toArrayUnsafe()))
        .isPresent();
  }
}
