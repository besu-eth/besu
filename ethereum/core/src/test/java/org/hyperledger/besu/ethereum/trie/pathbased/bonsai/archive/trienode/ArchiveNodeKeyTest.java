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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.archive.trienode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class ArchiveNodeKeyTest {
  @Test
  void accountKeyHasLengthPrefix() {
    assertThat(ArchiveNodeKey.account(Bytes.of(0x0e))).isEqualTo(Bytes.of(0x01, 0x0e));
  }

  @Test
  void lengthPrefixPreventsPrefixCollision() {
    // shallow [0x0e] vs deep [0x0e,0x00] must not be byte-prefixes of each other
    final Bytes shallow = ArchiveNodeKey.account(Bytes.of(0x0e));
    final Bytes deep = ArchiveNodeKey.account(Bytes.of(0x0e, 0x00));
    assertThat(shallow.commonPrefixLength(deep)).isEqualTo(0); // differ at the length byte
  }

  @Test
  void storageKeyPrependsAccountHash() {
    final Bytes32 acct = Bytes32.repeat((byte) 0x11);
    assertThat(ArchiveNodeKey.storage(acct, Bytes.of(0x0e)))
        .isEqualTo(Bytes.concatenate(acct, Bytes.of(0x01, 0x0e)));
  }

  @Test
  void historyKeyRoundTrips() {
    final Bytes nk = ArchiveNodeKey.account(Bytes.of(0x0e));
    final Bytes hk = ArchiveNodeKey.historyKey(nk, 42L);
    assertThat(ArchiveNodeKey.blockFromHistoryKey(hk)).isEqualTo(42L);
    assertThat(ArchiveNodeKey.naturalKeyFromHistoryKey(hk)).isEqualTo(nk);
  }

  @Test
  void rejectsOversizeLocation() {
    assertThatThrownBy(() -> ArchiveNodeKey.account(Bytes.repeat((byte) 1, 256)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
