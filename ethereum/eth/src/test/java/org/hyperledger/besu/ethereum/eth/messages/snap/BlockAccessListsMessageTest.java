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

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;

import java.math.BigInteger;
import java.util.List;

import org.junit.jupiter.api.Test;

public class BlockAccessListsMessageTest {

  @Test
  public void roundTripWithRequestId() {
    final List<BlockAccessList> expected = List.of(new BlockAccessList(List.of()));

    final BlockAccessListsMessage initialMessage = BlockAccessListsMessage.create(expected);
    final MessageData wrapped = initialMessage.wrapMessageData(BigInteger.valueOf(11));
    final MessageData raw = new RawMessage(SnapV2.BLOCK_ACCESS_LISTS, wrapped.getData());

    final BlockAccessListsMessage message = BlockAccessListsMessage.readFrom(raw);

    assertThat(message.blockAccessLists(true)).containsExactlyElementsOf(expected);
  }

  @Test
  public void roundTripWithNoBlockAccessLists() {
    final BlockAccessListsMessage initialMessage = BlockAccessListsMessage.create(List.of());
    final RawMessage raw = new RawMessage(SnapV2.BLOCK_ACCESS_LISTS, initialMessage.getData());

    final BlockAccessListsMessage message = BlockAccessListsMessage.readFrom(raw);
    assertThat(message.blockAccessLists(false)).isEmpty();
  }
}
