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

import static org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage.MAX_PATH_SIZE;
import static org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage.MAX_TOTAL_PATHS;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class GetTrieNodeMessageTest {

  @Test
  public void roundTripTest() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<Bytes> paths = new ArrayList<>();
    final int hashCount = 20;
    for (int i = 0; i < hashCount; ++i) {
      paths.add(Bytes32.random());
    }

    // Perform round-trip transformation
    final MessageData initialMessage = GetTrieNodesMessage.create(rootHash, List.of(paths));
    final MessageData raw = new RawMessage(SnapV1.GET_TRIE_NODES, initialMessage.getData());

    final GetTrieNodesMessage message = GetTrieNodesMessage.readFrom(raw);

    // check match originals.
    final GetTrieNodesMessage.TrieNodesPaths response = message.paths(false);
    Assertions.assertThat(response.worldStateRootHash()).isEqualTo(rootHash);
    Assertions.assertThat(response.paths()).contains(paths);
    Assertions.assertThat(response.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void totalPathsAtLimitDecodesFully() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // Create exactly MAX_TOTAL_PATHS groups with 1 path each
    final List<List<Bytes>> groups =
        IntStream.range(0, MAX_TOTAL_PATHS).mapToObj(i -> List.of(Bytes.of((byte) i))).toList();

    final MessageData raw =
        new RawMessage(
            SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData());
    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(raw).paths(false);

    int totalPaths = result.paths().stream().mapToInt(List::size).sum();
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void totalPathsExceedingLimitAreTruncated() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 100 groups x 100 paths = 10,000 total paths, well over the 1,024 limit
    final List<List<Bytes>> groups =
        IntStream.range(0, 100)
            .mapToObj(i -> IntStream.range(0, 100).mapToObj(j -> Bytes.of((byte) j)).toList())
            .toList();

    final MessageData raw =
        new RawMessage(
            SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData());
    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(raw).paths(false);

    int totalPaths = result.paths().stream().mapToInt(List::size).sum();
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void manyGroupsFewPathsCapsAtTotalLimit() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 2,000 groups x 1 path = 2,000 total, should be capped at 1,024
    final List<List<Bytes>> groups =
        IntStream.range(0, 2000).mapToObj(i -> List.of(Bytes.of((byte) 0x01))).toList();

    final MessageData raw =
        new RawMessage(
            SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData());
    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(raw).paths(false);

    int totalPaths = result.paths().stream().mapToInt(List::size).sum();
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
    Assertions.assertThat(result.paths().size()).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void manyEmptyGroupsCappedAtTotalLimit() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 2,000 empty groups — each should still count toward the total cap
    final List<List<Bytes>> groups =
        IntStream.range(0, 2000).mapToObj(i -> List.<Bytes>of()).toList();

    final MessageData raw =
        new RawMessage(
            SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData());
    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(raw).paths(false);

    // Empty groups produce 0 paths but should be capped at MAX_TOTAL_PATHS groups
    Assertions.assertThat(result.paths().size()).isEqualTo(MAX_TOTAL_PATHS);
    int totalPaths = result.paths().stream().mapToInt(List::size).sum();
    Assertions.assertThat(totalPaths).isZero();
  }

  @Test
  public void pathsAtMaxSizeAreAccepted() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final Bytes maxSizePath = Bytes.of(new byte[MAX_PATH_SIZE]);
    final List<List<Bytes>> groups = List.of(List.of(maxSizePath));

    final MessageData raw =
        new RawMessage(
            SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData());
    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(raw).paths(false);

    Assertions.assertThat(result.paths().size()).isEqualTo(1);
    Assertions.assertThat(result.paths().get(0).get(0)).isEqualTo(maxSizePath);
  }

  @Test
  public void wrapRoundTripTest() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<Bytes> paths = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      paths.add(Bytes32.random());
    }

    final GetTrieNodesMessage initialMessage = GetTrieNodesMessage.create(rootHash, List.of(paths));
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(42));
    final MessageData raw = new RawMessage(SnapV1.GET_TRIE_NODES, wrapped.getData());

    final GetTrieNodesMessage message = GetTrieNodesMessage.readFrom(raw);

    final GetTrieNodesMessage.TrieNodesPaths response = message.paths(true);
    Assertions.assertThat(response.worldStateRootHash()).isEqualTo(rootHash);
    Assertions.assertThat(response.paths()).contains(paths);
    Assertions.assertThat(response.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }
}
