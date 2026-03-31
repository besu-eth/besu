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
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public final class GetTrieNodesMessage extends AbstractSnapMessageData {

  // Compact-encoded Keccak256 hash is at most 33 bytes (1 metadata + 32 data)
  static final int MAX_PATH_SIZE = 33;
  // Maximum total paths decoded across all groups, matches geth's maxTrieNodeLookups
  static final int MAX_TOTAL_PATHS = 1024;

  public GetTrieNodesMessage(final Bytes data) {
    super(data);
  }

  public static GetTrieNodesMessage readFrom(final MessageData message) {
    if (message instanceof GetTrieNodesMessage) {
      return (GetTrieNodesMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_TRIE_NODES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetTrieNodes.", code));
    }
    return new GetTrieNodesMessage(message.getData());
  }

  public static GetTrieNodesMessage create(
      final Hash worldStateRootHash, final List<List<Bytes>> paths) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(worldStateRootHash.getBytes());
    tmp.writeList(
        paths,
        (path, rlpOutput) ->
            rlpOutput.writeList(path, (b, subRlpOutput) -> subRlpOutput.writeBytes(b)));
    tmp.writeBigIntegerScalar(SIZE_REQUEST);
    tmp.endList();
    return new GetTrieNodesMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.GET_TRIE_NODES;
  }

  /**
   * Decode paths from an incoming peer message with limits to prevent memory amplification attacks.
   * Caps total paths at MAX_TOTAL_PATHS and rejects individual paths exceeding MAX_PATH_SIZE.
   */
  public TrieNodesPaths paths(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();

    final Hash rootHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));

    // Decode paths with a total cap and per-path size validation to prevent
    // memory amplification attacks (a ~16 MB wire message could otherwise cause ~1 GB of heap)
    final List<List<Bytes>> pathGroups = new ArrayList<>();
    int totalPaths = 0;
    boolean aborted = false;

    input.enterList();
    while (!input.isEndOfCurrentList() && totalPaths < MAX_TOTAL_PATHS && !aborted) {
      input.enterList();
      final List<Bytes> group = new ArrayList<>();
      while (!input.isEndOfCurrentList() && totalPaths < MAX_TOTAL_PATHS) {
        final Bytes path = input.readBytes();
        if (path.size() > MAX_PATH_SIZE) {
          aborted = true;
          break;
        }
        group.add(path);
        totalPaths++;
      }
      // skip any remaining paths in this group
      while (!input.isEndOfCurrentList()) {
        input.skipNext();
      }
      input.leaveList();
      // Add the group unless it was aborted due to an oversized path with no valid entries
      if (!aborted || !group.isEmpty()) {
        pathGroups.add(group);
      }
      // empty groups still count toward the limit to prevent empty-group flooding
      if (group.isEmpty()) {
        totalPaths++;
      }
    }
    // skip any remaining groups
    while (!input.isEndOfCurrentList()) {
      input.skipNext();
    }
    input.leaveList();

    final BigInteger responseBytes = input.readBigIntegerScalar();
    input.leaveList();

    return ImmutableTrieNodesPaths.builder()
        .worldStateRootHash(rootHash)
        .paths(pathGroups)
        .responseBytes(responseBytes)
        .build();
  }

  @Value.Immutable
  public interface TrieNodesPaths {

    Hash worldStateRootHash();

    List<List<Bytes>> paths();

    BigInteger responseBytes();
  }
}
