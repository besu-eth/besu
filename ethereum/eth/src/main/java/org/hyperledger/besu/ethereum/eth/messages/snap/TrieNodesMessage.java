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

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.List;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;

public final class TrieNodesMessage extends AbstractSnapMessageData {

  public TrieNodesMessage(final Bytes data) {
    super(data);
  }

  public static TrieNodesMessage readFrom(final MessageData message) {
    if (message instanceof TrieNodesMessage) {
      return (TrieNodesMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.TRIE_NODES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a TrieNodes.", code));
    }
    return new TrieNodesMessage(message.getData());
  }

  public static TrieNodesMessage create(final List<Bytes> nodes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeList(nodes, (node, rlpOutput) -> rlpOutput.writeBytes(node));
    tmp.endList();
    return new TrieNodesMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.TRIE_NODES;
  }

  public ArrayDeque<Bytes> nodes(final boolean withRequestId) {
    final ArrayDeque<Bytes> trieNodes = new ArrayDeque<>();
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      trieNodes.add(input.readBytes());
    }
    input.leaveList();
    input.leaveList();
    return trieNodes;
  }
}
