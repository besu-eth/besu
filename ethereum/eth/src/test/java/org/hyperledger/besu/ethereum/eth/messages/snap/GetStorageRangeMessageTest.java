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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.trie.RangeManager;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class GetStorageRangeMessageTest {

  @Test
  public void roundTripTest() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<Bytes32> accountKeys = List.of(Bytes32.random());
    final Bytes32 startKeyHash = RangeManager.MIN_RANGE;
    final Bytes32 endKeyHash = RangeManager.MAX_RANGE;

    final MessageData initialMessage =
        GetStorageRangeMessage.create(rootHash, accountKeys, startKeyHash, endKeyHash);
    final MessageData raw = new RawMessage(SnapV1.GET_STORAGE_RANGE, initialMessage.getData());

    final GetStorageRangeMessage.StorageRange range =
        GetStorageRangeMessage.readFrom(raw).range(false);

    Assertions.assertThat(range.worldStateRootHash()).isEqualTo(rootHash);
    final List<Bytes32> accountHashes = new ArrayList<>();
    range.accountHashes().forEach(accountHashes::add);
    Assertions.assertThat(accountHashes).isEqualTo(accountKeys);
    Assertions.assertThat(range.startKeyHash().getBytes()).isEqualTo(startKeyHash);
    Assertions.assertThat(range.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void wrapRoundTripTest() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<Bytes32> accountKeys = List.of(Bytes32.random());
    final Bytes32 startKeyHash = RangeManager.MIN_RANGE;
    final Bytes32 endKeyHash = RangeManager.MAX_RANGE;

    final GetStorageRangeMessage initialMessage =
        GetStorageRangeMessage.create(rootHash, accountKeys, startKeyHash, endKeyHash);
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(42));
    final MessageData raw = new RawMessage(SnapV1.GET_STORAGE_RANGE, wrapped.getData());

    final GetStorageRangeMessage.StorageRange range =
        GetStorageRangeMessage.readFrom(raw).range(true);

    Assertions.assertThat(range.worldStateRootHash()).isEqualTo(rootHash);
    final List<Bytes32> accountHashes = new ArrayList<>();
    range.accountHashes().forEach(accountHashes::add);
    Assertions.assertThat(accountHashes).isEqualTo(accountKeys);
    Assertions.assertThat(range.startKeyHash().getBytes()).isEqualTo(startKeyHash);
    Assertions.assertThat(range.endKeyHash().getBytes()).isEqualTo(endKeyHash);
    Assertions.assertThat(range.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void lazyIterationStaysIndependentAcrossCalls() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final Bytes32 hash1 = Bytes32.random();
    final Bytes32 hash2 = Bytes32.random();
    final Bytes32 hash3 = Bytes32.random();

    final GetStorageRangeMessage.StorageRange range =
        GetStorageRangeMessage.create(
                rootHash,
                List.of(hash1, hash2, hash3),
                RangeManager.MIN_RANGE,
                RangeManager.MAX_RANGE)
            .range(false);

    // Two independent iterations must each see all three hashes in order.
    final List<Bytes32> first = new ArrayList<>();
    range.accountHashes().forEach(first::add);
    final List<Bytes32> second = new ArrayList<>();
    range.accountHashes().forEach(second::add);

    Assertions.assertThat(first).containsExactly(hash1, hash2, hash3);
    Assertions.assertThat(second).containsExactly(hash1, hash2, hash3);
  }

  @Test
  public void peekIteratorDoesNotAffectMainIteration() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final Bytes32 hash1 = Bytes32.random();
    final Bytes32 hash2 = Bytes32.random();

    final GetStorageRangeMessage.StorageRange range =
        GetStorageRangeMessage.create(
                rootHash, List.of(hash1, hash2), RangeManager.MIN_RANGE, RangeManager.MAX_RANGE)
            .range(false);

    // Simulate the multi-account peek done by SnapServer.
    final var peekIter = range.accountHashes().iterator();
    if (peekIter.hasNext()) peekIter.next();
    Assertions.assertThat(peekIter.hasNext()).isTrue(); // second element exists

    // A fresh iterable from the same StorageRange must still see both hashes.
    final List<Bytes32> all = new ArrayList<>();
    range.accountHashes().forEach(all::add);
    Assertions.assertThat(all).containsExactly(hash1, hash2);
  }
}
