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
package org.hyperledger.besu.ethereum.eth.sync.snap2.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.AccountRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.storage.Accounts;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProofs;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.common.PatriciaTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

class AccountRangeRequestTest {

  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie =
      new SimpleMerklePatriciaTrie<>(Function.identity());
  private final TreeMap<Bytes32, Bytes> accounts = new TreeMap<>();

  AccountRangeRequestTest() {
    final Random random = new Random(2);
    for (int i = 0; i < 10; i++) {
      final Bytes32 hash = Bytes32.random(random);
      accounts.put(hash, account(i));
      trie.put(hash, account(i));
    }
  }

  @Test
  void asksForTheRangeAtTheStateRootOfThePivot() {
    final AccountRangeRequest request = request(RangeManager.MIN_RANGE);

    assertThat(request.message(peer()).getData())
        .isEqualTo(
            SnapMessages.getAccountRange(
                    trie.getRootHash(), RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, 4096)
                .getData());
    assertThat(request.minimumPeerHeight()).isEqualTo(1);
  }

  @Test
  void acceptsAProvenRangeAndTellsThatMoreFollow() {
    final NavigableMap<Bytes32, Bytes> firstHalf =
        accounts.headMap(new ArrayList<>(accounts.keySet()).get(4), true);

    final AccountRangeRequest.Result result =
        request(RangeManager.MIN_RANGE).decode(response(firstHalf), peer());

    assertThat(result.hashes()).containsExactlyElementsOf(firstHalf.keySet());
    assertThat(result.accounts()).containsExactlyElementsOf(firstHalf.values());
    assertThat(result.hasMore()).isTrue();
    assertThat(result.startsWith(firstHalf.firstKey())).isTrue();
    assertThat(result.startsWith(firstHalf.lastKey())).isFalse();
  }

  @Test
  void tellsWhenTheRangeReachesTheLastAccount() {
    assertThat(request(RangeManager.MIN_RANGE).decode(response(accounts), peer()).hasMore())
        .isFalse();
  }

  // the last range of the key space may hold no account, which the proof of its origin shows
  @Test
  void completesARangeWhoseProofShowsNoAccountFromTheOrigin() {
    final Bytes32 origin = KeyRangeSet.next(accounts.lastKey()).orElseThrow();
    final MessageData proofOnly =
        AccountRangeMessage.create(new TreeMap<>(), RangeProofs.proof(trie, origin, origin))
            .wrapMessageData(BigInteger.ONE);

    final AccountRangeRequest.Result result = request(origin).decode(proofOnly, peer());

    assertThat(result.hashes()).isEmpty();
    assertThat(result.hasMore()).isFalse();
  }

  @Test
  void rejectsAModifiedAccount() {
    final TreeMap<Bytes32, Bytes> modified = new TreeMap<>(accounts);
    modified.put(modified.firstKey(), account(99));

    assertThatThrownBy(() -> request(RangeManager.MIN_RANGE).decode(response(modified), peer()))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  @Test
  void treatsAnAnswerWithoutAccountsOrProofAsEmpty() {
    final MessageData nothing =
        AccountRangeMessage.create(new TreeMap<>(), List.of()).wrapMessageData(BigInteger.ONE);

    assertThatThrownBy(() -> request(RangeManager.MIN_RANGE).decode(nothing, peer()))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  private AccountRangeRequest request(final Bytes32 origin) {
    final Pivot pivot =
        new Pivot(
            0,
            new BlockHeaderTestFixture()
                .number(1)
                .stateRoot(Hash.wrap(trie.getRootHash()))
                .buildHeader());
    return new AccountRangeRequest(pivot, origin, RangeManager.MAX_RANGE, 4096, "account_range");
  }

  // the accounts, with the proofs of the origin and of the last account
  private MessageData response(final NavigableMap<Bytes32, Bytes> served) {
    return AccountRangeMessage.create(
            served, RangeProofs.proof(trie, RangeManager.MIN_RANGE, served.lastKey()))
        .wrapMessageData(BigInteger.ONE);
  }

  private static Bytes account(final long nonce) {
    return Accounts.encode(
        new PatriciaTrieAccountValue(nonce, Wei.of(nonce + 1), Hash.EMPTY_TRIE_HASH, Hash.EMPTY));
  }

  private static EthPeer peer() {
    return mock(EthPeer.class);
  }
}
