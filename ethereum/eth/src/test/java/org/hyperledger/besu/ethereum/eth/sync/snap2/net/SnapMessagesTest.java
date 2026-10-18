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
package org.hyperledger.besu.ethereum.eth.sync.snap2.net;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;

import java.math.BigInteger;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class SnapMessagesTest {

  private static final BigInteger REQUEST_ID = BigInteger.valueOf(7);

  @Test
  void decodesAccountRangeResponses() {
    final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();
    accounts.put(
        Bytes32.random(),
        RLP.encode(
            new PatriciaTrieAccountValue(3, Wei.of(10), Hash.EMPTY_TRIE_HASH, Hash.EMPTY)
                ::writeTo));
    accounts.put(
        Bytes32.random(),
        RLP.encode(
            new PatriciaTrieAccountValue(
                    1, Wei.of(7), Hash.hash(Bytes.of(1)), Hash.hash(Bytes.of(2)))
                ::writeTo));
    final List<Bytes> proof = List.of(Bytes.of(1, 2, 3), Bytes.of(4, 5));
    final MessageData wire =
        AccountRangeMessage.create(accounts, proof).wrapMessageData(REQUEST_ID);

    final SnapMessages.AccountRange decoded = SnapMessages.accountRange(wire);

    assertThat(decoded.hashes()).containsExactlyElementsOf(accounts.keySet());
    assertThat(decoded.accounts()).containsExactlyElementsOf(accounts.values());
    assertThat(decoded.proof()).containsExactlyElementsOf(proof);
  }

  @Test
  void decodesStorageResponses() {
    final kotlin.collections.ArrayDeque<NavigableMap<Bytes32, Bytes>> slots =
        new kotlin.collections.ArrayDeque<>();
    final TreeMap<Bytes32, Bytes> first = new TreeMap<>();
    first.put(Bytes32.random(), RLP.encodeOne(Bytes.of(5)));
    first.put(Bytes32.random(), RLP.encodeOne(Bytes.of(1, 0, 0)));
    slots.add(first);
    slots.add(new TreeMap<>());
    final MessageData wire =
        StorageRangeMessage.create(slots, List.of(Bytes.of(9))).wrapMessageData(REQUEST_ID);

    final SnapMessages.StorageRanges decoded = SnapMessages.storageRanges(wire);

    assertThat(decoded.slotHashes()).hasSize(2);
    assertThat(decoded.slotHashes().get(0)).containsExactlyElementsOf(first.keySet());
    assertThat(decoded.slotValues().get(0)).containsExactlyElementsOf(first.values());
    assertThat(decoded.slotHashes().get(1)).isEmpty();
    assertThat(decoded.proof()).containsExactly(Bytes.of(9));
  }
}
