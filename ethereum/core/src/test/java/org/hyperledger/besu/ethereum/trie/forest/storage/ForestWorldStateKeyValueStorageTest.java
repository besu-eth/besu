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

import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ForestWorldStateKeyValueStorageTest {

  private ForestWorldStateKeyValueStorage newStorage() {
    return new ForestWorldStateKeyValueStorage(new InMemoryKeyValueStorage());
  }

  @Test
  void worldStateRoot_absentByDefault() {
    assertThat(newStorage().getWorldStateRoot()).isEmpty();
  }

  @Test
  void worldStateRoot_roundTrips() {
    final ForestWorldStateKeyValueStorage storage = newStorage();
    final Bytes32 root = Bytes32.fromHexString("0x" + "ab".repeat(32));
    final ForestWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.putWorldStateRoot(root);
    updater.commit();
    assertThat(storage.getWorldStateRoot()).contains(root);
  }

  @Test
  void prune_doesNotDeleteRootPointer() {
    final ForestWorldStateKeyValueStorage storage = newStorage();
    final Bytes32 root = Bytes32.fromHexString("0x" + "cd".repeat(32));
    final ForestWorldStateKeyValueStorage.Updater updater = storage.updater();
    updater.putAccountStateTrieNode(root, Bytes.fromHexString("0xdeadbeef"));
    updater.putWorldStateRoot(root);
    updater.commit();

    // inUseCheck marks nothing in use -> the node is pruned, the pointer must survive.
    final long pruned = storage.prune(key -> false);

    assertThat(pruned).isEqualTo(1L); // only the 32-byte node
    assertThat(storage.getWorldStateRoot()).contains(root);
  }
}
