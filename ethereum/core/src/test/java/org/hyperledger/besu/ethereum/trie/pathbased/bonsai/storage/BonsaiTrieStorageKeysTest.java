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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY;

import org.hyperledger.besu.datatypes.Hash;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class BonsaiTrieStorageKeysTest {

  @Test
  void classifiesAccountAndStorageTrieKeys() {
    assertThat(BonsaiTrieStorageKeys.isAccountTrieLocationKey(Bytes.EMPTY.toArrayUnsafe()))
        .isTrue();
    assertThat(BonsaiTrieStorageKeys.isAccountTrieLocationKey(Bytes.fromHexString("0x0102").toArrayUnsafe()))
        .isTrue();
    assertThat(BonsaiTrieStorageKeys.isMetadataKey(WORLD_ROOT_HASH_KEY)).isTrue();

    final Hash accountHash = Hash.wrap(Bytes32.repeat((byte) 0xab));
    final byte[] storageKey =
        BonsaiTrieStorageKeys.storageTrieKey(accountHash, Bytes.fromHexString("0x01"));
    assertThat(BonsaiTrieStorageKeys.isStorageTrieKey(storageKey)).isTrue();
    assertThat(BonsaiTrieStorageKeys.isAccountTrieLocationKey(storageKey)).isFalse();
  }
}
