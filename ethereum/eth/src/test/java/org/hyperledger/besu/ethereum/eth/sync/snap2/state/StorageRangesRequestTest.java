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
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.messages.snap.StorageRangeMessage;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SnapMessages;
import org.hyperledger.besu.ethereum.eth.sync.snap2.net.SyncRequest;
import org.hyperledger.besu.ethereum.eth.sync.snap2.trie.RangeProofs;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.SimpleMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;

class StorageRangesRequestTest {

  private static final Bytes32 ACCOUNT = Bytes32.fromHexString("0x01");
  private static final Bytes32 OTHER_ACCOUNT = Bytes32.fromHexString("0x02");
  private static final Hash STATE_ROOT = Hash.hash(Bytes.of(9));
  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> storage =
      new SimpleMerklePatriciaTrie<>(Function.identity());
  private final SimpleMerklePatriciaTrie<Bytes32, Bytes> other =
      new SimpleMerklePatriciaTrie<>(Function.identity());

  StorageRangesRequestTest() {
    for (int i = 1; i <= 3; i++) {
      storage.put(slot(i * 0x40), RLP.encodeOne(UInt256.valueOf(i).trimLeadingZeros()));
    }
    for (int i = 1; i <= 4; i++) {
      other.put(slot(i * 0x30), RLP.encodeOne(UInt256.valueOf(10 + i).trimLeadingZeros()));
    }
  }

  @Test
  void asksForTheStoragesAtTheStateRootOfThePivot() {
    final StorageRangesRequest request = twoStorages(RangeManager.MIN_RANGE);

    assertThat(request.message(peer()).getData())
        .isEqualTo(
            SnapMessages.getStorageRanges(
                    Bytes32.wrap(STATE_ROOT.getBytes()),
                    List.of(ACCOUNT, OTHER_ACCOUNT),
                    RangeManager.MIN_RANGE,
                    RangeManager.MAX_RANGE,
                    4096)
                .getData());
    assertThat(request.minimumPeerHeight()).isEqualTo(1);
  }

  // a peer may answer a range without slots with no slot list, only the proof of the origin
  @Test
  void completesARangeWhoseProofShowsNoSlotFromTheOrigin() {
    final Bytes32 origin = slot(0xd0);

    final StorageRangesRequest.Result result = chunk(origin).decode(proofOnly(origin), peer());

    assertThat(result.accounts()).hasSize(1);
    assertThat(result.accounts().getFirst().hashes()).isEmpty();
    assertThat(result.accounts().getFirst().complete()).isTrue();
  }

  @Test
  void rejectsAProofThatHidesSlotsAfterTheOrigin() {
    final Bytes32 origin = slot(0x50);

    assertThatThrownBy(() -> chunk(origin).decode(proofOnly(origin), peer()))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  @Test
  void treatsAnAnswerWithoutSlotsOrProofAsEmpty() {
    final MessageData nothing =
        StorageRangeMessage.create(new ArrayDeque<>(), List.of()).wrapMessageData(BigInteger.ONE);

    assertThatThrownBy(() -> chunk(slot(0xd0)).decode(nothing, peer()))
        .isInstanceOf(SyncRequest.EmptyResponseException.class);
  }

  @Test
  void rejectsMoreStoragesThanRequested() {
    final MessageData twoStorages =
        response(List.of(slots(storage, 3), slots(other, 4)), List.of());

    assertThatThrownBy(() -> chunk(RangeManager.MIN_RANGE).decode(twoStorages, peer()))
        .isInstanceOf(SyncRequest.InvalidResponseException.class)
        .hasMessageContaining("more storage ranges than requested");
  }

  @Test
  void rejectsAnOriginOrALimitForSeveralStorages() {
    assertThatThrownBy(() -> twoStorages(slot(0x40)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("an origin for several storages");
    // servers ignore it, so the slots past it would be asked again
    assertThatThrownBy(() -> twoStorages(RangeManager.MIN_RANGE, slot(0x40)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("a limit for several storages");
  }

  // the proof covers only the last storage, which the response cuts short
  @Test
  void acceptsACompleteStorageThenOneCutShortWithItsProof() {
    final NavigableMap<Bytes32, Bytes> firstTwo = slots(other, 2);
    final List<Bytes> proof = RangeProofs.proof(other, RangeManager.MIN_RANGE, firstTwo.lastKey());

    final StorageRangesRequest.Result result =
        twoStorages(RangeManager.MIN_RANGE)
            .decode(response(List.of(slots(storage, 3), firstTwo), proof), peer());

    assertThat(result.accounts()).hasSize(2);
    assertThat(result.accounts().get(0).complete()).isTrue();
    assertThat(result.accounts().get(1).hashes()).containsExactlyElementsOf(firstTwo.keySet());
    assertThat(result.accounts().get(1).complete()).isFalse();
    // each range comes with the trie nodes its verification rebuilt, the root last
    assertThat(trie(result.accounts().get(0)).entriesFrom(RangeManager.MIN_RANGE, 3))
        .isEqualTo(slots(storage, 3));
    final StorageRangesRequest.TrieNode root = result.accounts().get(1).nodes().getLast();
    assertThat(root.location()).isEqualTo(Bytes.EMPTY);
    assertThat(root.hash()).isEqualTo(other.getRootHash());
  }

  // the root of a task can move on while its request is in flight
  @Test
  void verifiesALateAnswerAgainstTheRootItWasAskedAt() {
    final StorageTask task = new StorageTask(ACCOUNT, storage.getRootHash());
    final StorageRangesRequest request =
        new StorageRangesRequest(
            pivot(),
            List.of(task),
            RangeManager.MIN_RANGE,
            RangeManager.MAX_RANGE,
            4096,
            "storage_ranges");
    task.refreshRoot(other.getRootHash());

    final StorageRangesRequest.Result result =
        request.decode(response(List.of(slots(storage, 3)), List.of()), peer());

    assertThat(result.accounts().getFirst().root()).isEqualTo(storage.getRootHash());
    assertThat(result.accounts().getFirst().complete()).isTrue();
  }

  @Test
  void rejectsAStorageCutShortWithoutItsProof() {
    final MessageData cutShort = response(List.of(slots(storage, 3), slots(other, 2)), List.of());

    assertThatThrownBy(() -> twoStorages(RangeManager.MIN_RANGE).decode(cutShort, peer()))
        .isInstanceOf(SyncRequest.InvalidResponseException.class);
  }

  // the storages of two accounts from an origin, as a batch asks them from the first key
  private StorageRangesRequest twoStorages(final Bytes32 origin) {
    return twoStorages(origin, RangeManager.MAX_RANGE);
  }

  private StorageRangesRequest twoStorages(final Bytes32 origin, final Bytes32 limit) {
    return new StorageRangesRequest(
        pivot(),
        List.of(
            new StorageTask(ACCOUNT, storage.getRootHash()),
            new StorageTask(OTHER_ACCOUNT, other.getRootHash())),
        origin,
        limit,
        4096,
        "storage_ranges");
  }

  // the first slots of a storage, in key order
  private static NavigableMap<Bytes32, Bytes> slots(
      final SimpleMerklePatriciaTrie<Bytes32, Bytes> trie, final int count) {
    return new TreeMap<>(trie.entriesFrom(RangeManager.MIN_RANGE, count));
  }

  // the trie of a range, read from the nodes its verification rebuilt
  private static MerkleTrie<Bytes32, Bytes> trie(final StorageRangesRequest.Slots slots) {
    final Map<Bytes32, Bytes> nodes = new HashMap<>();
    slots.nodes().forEach(node -> nodes.put(node.hash(), node.rlp()));
    return new StoredMerklePatriciaTrie<>(
        (location, hash) -> Optional.ofNullable(nodes.get(hash)),
        slots.root(),
        Function.identity(),
        Function.identity());
  }

  private static MessageData response(
      final List<NavigableMap<Bytes32, Bytes>> slots, final List<Bytes> proof) {
    return StorageRangeMessage.create(new ArrayDeque<>(slots), proof)
        .wrapMessageData(BigInteger.ONE);
  }

  private StorageRangesRequest chunk(final Bytes32 origin) {
    return new StorageRangesRequest(
        pivot(),
        List.of(new StorageTask(ACCOUNT, storage.getRootHash())),
        origin,
        RangeManager.MAX_RANGE,
        4096,
        "storage_chunk");
  }

  private static Pivot pivot() {
    return new Pivot(0, new BlockHeaderTestFixture().number(1).stateRoot(STATE_ROOT).buildHeader());
  }

  private MessageData proofOnly(final Bytes32 origin) {
    return StorageRangeMessage.create(
            new ArrayDeque<>(), RangeProofs.proof(storage, origin, origin))
        .wrapMessageData(BigInteger.ONE);
  }

  private static Bytes32 slot(final int firstByte) {
    return Bytes32.wrap(Bytes.concatenate(Bytes.of(firstByte), Bytes.wrap(new byte[31])));
  }

  private static EthPeer peer() {
    return mock(EthPeer.class);
  }
}
