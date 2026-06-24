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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage.MAX_PATH_SIZE;
import static org.hyperledger.besu.ethereum.eth.messages.snap.GetTrieNodesMessage.MAX_TOTAL_PATHS;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public final class GetTrieNodeMessageTest {

  /** Collect all groups from the iterable into a nested list for easy assertion. */
  private static List<List<Bytes>> collect(final Iterable<Iterable<Bytes>> paths) {
    final List<List<Bytes>> result = new ArrayList<>();
    for (final Iterable<Bytes> group : paths) {
      final List<Bytes> groupList = new ArrayList<>();
      group.forEach(groupList::add);
      result.add(groupList);
    }
    return result;
  }

  @Test
  public void roundTripTest() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<Bytes> paths = new ArrayList<>();
    for (int i = 0; i < 20; ++i) {
      paths.add(Bytes32.random());
    }

    final MessageData initialMessage = GetTrieNodesMessage.create(rootHash, List.of(paths));
    final MessageData raw = new RawMessage(SnapV1.GET_TRIE_NODES, initialMessage.getData());

    final GetTrieNodesMessage.TrieNodesPaths response =
        GetTrieNodesMessage.readFrom(raw).paths(false);
    Assertions.assertThat(response.worldStateRootHash()).isEqualTo(rootHash);
    Assertions.assertThat(collect(response.paths())).containsExactly(paths);
    Assertions.assertThat(response.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void totalPathsAtLimitDecodesFully() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 1024 groups × 1 path = exactly MAX_TOTAL_PATHS paths; all should decode.
    final List<List<Bytes>> groups =
        IntStream.range(0, MAX_TOTAL_PATHS).mapToObj(i -> List.of(Bytes.of((byte) i))).toList();

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(
                new RawMessage(
                    SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()))
            .paths(false);

    int totalPaths = 0;
    for (final Iterable<Bytes> group : result.paths()) {
      for (final Bytes ignored : group) totalPaths++;
    }
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void totalPathsExceedingLimitAreTruncated() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 100 groups × 100 paths = 10,000 total; only MAX_TOTAL_PATHS should decode.
    final List<List<Bytes>> groups =
        IntStream.range(0, 100)
            .mapToObj(i -> IntStream.range(0, 100).mapToObj(j -> Bytes.of((byte) j)).toList())
            .toList();

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(
                new RawMessage(
                    SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()))
            .paths(false);

    int totalPaths = 0;
    for (final Iterable<Bytes> group : result.paths()) {
      for (final Bytes ignored : group) totalPaths++;
    }
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void manyGroupsFewPathsCapsAtTotalPaths() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // 2000 groups × 1 path = 2000 paths; capped at MAX_TOTAL_PATHS.
    final List<List<Bytes>> groups =
        IntStream.range(0, 2000).mapToObj(i -> List.of(Bytes.of((byte) 0x01))).toList();

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(
                new RawMessage(
                    SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()))
            .paths(false);

    int totalPaths = 0;
    for (final Iterable<Bytes> group : result.paths()) {
      for (final Bytes ignored : group) totalPaths++;
    }
    Assertions.assertThat(totalPaths).isEqualTo(MAX_TOTAL_PATHS);
  }

  @Test
  public void emptyGroupsDoNotConsumePathBudget() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    // Empty groups cost 0 budget — both geth and nethermind reject them immediately,
    // so no legitimate client sends them and they need no special flooding protection.
    final List<List<Bytes>> groups =
        IntStream.range(0, 2000).mapToObj(i -> List.<Bytes>of()).toList();

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(
                new RawMessage(
                    SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()))
            .paths(false);

    int totalGroups = 0;
    for (final Iterable<Bytes> ignored : result.paths()) {
      totalGroups++;
    }
    // All 2000 empty groups are yielded since they consume no path budget.
    Assertions.assertThat(totalGroups).isEqualTo(2000);
  }

  @Test
  public void oversizedPathThrowsRLPException() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final Bytes validPath = Bytes.of(new byte[MAX_PATH_SIZE]);
    final Bytes oversizedPath = Bytes.of(new byte[MAX_PATH_SIZE + 1]);
    final List<List<Bytes>> groups =
        List.of(List.of(validPath, validPath), List.of(oversizedPath), List.of(validPath));

    final GetTrieNodesMessage message =
        GetTrieNodesMessage.readFrom(
            new RawMessage(
                SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()));

    // exception is deferred until iteration reaches the oversized path
    assertThatThrownBy(
            () -> {
              for (final Iterable<Bytes> group : message.paths(false).paths()) {
                for (final Bytes ignored : group) {} // drain each group
              }
            })
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds maximum");
  }

  @Test
  public void pathsAtMaxSizeAreAccepted() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final Bytes maxSizePath = Bytes.of(new byte[MAX_PATH_SIZE]);
    final List<List<Bytes>> groups = List.of(List.of(maxSizePath));

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.readFrom(
                new RawMessage(
                    SnapV1.GET_TRIE_NODES, GetTrieNodesMessage.create(rootHash, groups).getData()))
            .paths(false);

    final List<List<Bytes>> decoded = collect(result.paths());
    Assertions.assertThat(decoded).hasSize(1);
    Assertions.assertThat(decoded.get(0)).containsExactly(maxSizePath);
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

    final GetTrieNodesMessage.TrieNodesPaths response =
        GetTrieNodesMessage.readFrom(raw).paths(true);
    Assertions.assertThat(response.worldStateRootHash()).isEqualTo(rootHash);
    Assertions.assertThat(collect(response.paths())).containsExactly(paths);
    Assertions.assertThat(response.responseBytes()).isEqualTo(AbstractSnapMessageData.SIZE_REQUEST);
  }

  @Test
  public void lazyIterationStaysIndependentAcrossCalls() {
    final Hash rootHash = Hash.wrap(Bytes32.random());
    final List<List<Bytes>> groups =
        List.of(
            List.of(Bytes.fromHexString("0x01")),
            List.of(Bytes.fromHexString("0x02")),
            List.of(Bytes.fromHexString("0x03")));

    final GetTrieNodesMessage.TrieNodesPaths result =
        GetTrieNodesMessage.create(rootHash, groups).paths(false);

    // Two independent calls to paths() must each produce a complete, equal result.
    final List<List<Bytes>> first = collect(result.paths());
    final List<List<Bytes>> second = collect(result.paths());

    Assertions.assertThat(first).isEqualTo(groups);
    Assertions.assertThat(second).isEqualTo(groups);
  }
}
