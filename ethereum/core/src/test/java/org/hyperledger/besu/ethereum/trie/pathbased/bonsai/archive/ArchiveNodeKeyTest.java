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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ArchiveNodeKeyTest {

  @Test
  void accountNaturalKeyIsLengthPrefixedLocation() {
    final Bytes location = Bytes.fromHexString("0x0102");
    // format: [len: 1 byte] | location
    assertThat(ArchiveNodeKey.account(location))
        .isEqualTo(Bytes.concatenate(Bytes.of((byte) location.size()), location));
  }

  @Test
  void storageNaturalKeyIsAccountHashThenLengthPrefixedLocation() {
    final Bytes32 accountHash = Bytes32.random();
    final Bytes location = Bytes.fromHexString("0x0a0b");
    // format: accountHash(32) | [len: 1 byte] | location
    assertThat(ArchiveNodeKey.storage(accountHash, location))
        .isEqualTo(Bytes.concatenate(accountHash, Bytes.of((byte) location.size()), location));
  }

  @Test
  void storageNaturalKeyRejectsWrongSizedAccountHash() {
    final Bytes tooShort = Bytes.fromHexString("0x0102");
    final Bytes location = Bytes.EMPTY;
    assertThatThrownBy(() -> ArchiveNodeKey.storage(tooShort, location))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("32");
  }

  @Test
  void historyKeyRoundTripsBlockAndNaturalKeyForBlockZero() {
    final Bytes naturalKey = Bytes.fromHexString("0xabcdef");
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, 0L);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(key)).isEqualTo(0L);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(key)).isEqualTo(naturalKey);
  }

  @Test
  void historyKeyRoundTripsBlockAndNaturalKeyForMaxBlock() {
    final Bytes naturalKey = Bytes32.random();
    final Bytes key = ArchiveNodeKey.historyKey(naturalKey, Long.MAX_VALUE);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(key)).isEqualTo(Long.MAX_VALUE);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(key)).isEqualTo(naturalKey);
  }

  @Test
  void accountNaturalKeyNoPrefixCollisionBetweenShallowAndDeepPaths() {
    // Without the length prefix, [0x0e][block=0] < [0x0e,0x00][block=0] < [0x0e][block=9],
    // so a deeper path's genesis entry would shadow a shallower path's getLatestBefore lookup.
    // With the length prefix: [0x01,0x0e,...] < [0x01,0x0e,block=9] < [0x02,0x0e,0x00,...],
    // so the deeper path's entries are always strictly greater than the shallower path's seek key.
    final Bytes shallow = ArchiveNodeKey.account(Bytes.fromHexString("0x0e"));
    final Bytes deep = ArchiveNodeKey.account(Bytes.fromHexString("0x0e00"));
    final Bytes shallowBlock0 = ArchiveNodeKey.historyKey(shallow, 0L);
    final Bytes shallowBlock9 = ArchiveNodeKey.historyKey(shallow, 9L);
    final Bytes deepBlock0 = ArchiveNodeKey.historyKey(deep, 0L);
    // shallowBlock0 < shallowBlock9 (ascending blocks)
    assertThat(shallowBlock0.compareTo(shallowBlock9)).isLessThan(0);
    // deepBlock0 > shallowBlock9 (deeper path sorts entirely after shallower path's entries)
    assertThat(deepBlock0.compareTo(shallowBlock9)).isGreaterThan(0);
  }

  @Test
  void historyKeySortsByNaturalKeyThenByBlockAscending() {
    // Same natural key, ascending blocks must sort ascending lexicographically —
    // this is the property getNearestBefore/seekForPrev relies on for every later task.
    final Bytes naturalKey = Bytes.fromHexString("0x01");
    final Bytes key10 = ArchiveNodeKey.historyKey(naturalKey, 10L);
    final Bytes key11 = ArchiveNodeKey.historyKey(naturalKey, 11L);
    assertThat(key10.compareTo(key11)).isLessThan(0);
  }

  @Test
  void blockFromHistoryKeyRejectsTooShortInput() {
    assertThatThrownBy(() -> ArchiveNodeKey.blockFromHistoryKey(Bytes.fromHexString("0x0102")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
