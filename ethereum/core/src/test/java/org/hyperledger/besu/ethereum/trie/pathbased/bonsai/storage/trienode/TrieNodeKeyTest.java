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

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class TrieNodeKeyTest {

  private final Hash accountHash = Hash.hash(Bytes.of(0xAA));

  @Test
  void keyWithTriePrefixIsAccountHashFollowedByLocation() {
    final Bytes location = Bytes.of(0x01, 0x02);
    assertThat(TrieNodeKey.of(accountHash, location))
        .isEqualTo(Bytes.concatenate(accountHash.getBytes(), location));
  }

  @Test
  void locationWithoutTriePrefixIsTheKeyItself() {
    final Bytes location = Bytes.of(0x01, 0x02);
    assertThat(TrieNodeKey.hasTriePrefix(location)).isFalse();
    assertThat(TrieNodeKey.location(location)).isEqualTo(location);
    assertThat(TrieNodeKey.hasTriePrefix(Bytes.EMPTY)).isFalse();
  }

  @Test
  void keyWithTriePrefixSplitsIntoPrefixAndLocation() {
    final Bytes location = Bytes.of(0x01, 0x02);
    final Bytes key = TrieNodeKey.of(accountHash, location);
    assertThat(TrieNodeKey.hasTriePrefix(key)).isTrue();
    assertThat(TrieNodeKey.triePrefix(key)).isEqualTo(accountHash.getBytes());
    assertThat(TrieNodeKey.location(key)).isEqualTo(location);
  }

  @Test
  void storageTrieRootKeyHasTriePrefixAndEmptyLocation() {
    final Bytes key = TrieNodeKey.of(accountHash, Bytes.EMPTY);
    assertThat(TrieNodeKey.hasTriePrefix(key)).isTrue();
    assertThat(TrieNodeKey.location(key)).isEqualTo(Bytes.EMPTY);
  }
}
