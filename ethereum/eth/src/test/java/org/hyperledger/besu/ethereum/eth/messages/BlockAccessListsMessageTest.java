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
package org.hyperledger.besu.ethereum.eth.messages;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.RawMessage;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

public class BlockAccessListsMessageTest {

  @Test
  public void roundTripTest() {
    final BlockDataGenerator generator = new BlockDataGenerator(1);
    final List<BlockAccessList> expected =
        List.of(generator.blockAccessList(), new BlockAccessList(List.of()));

    final BlockAccessListsMessage initialMessage =
        BlockAccessListsMessage.createFromBlockAccessLists(expected);
    final RawMessage raw =
        new RawMessage(EthProtocolMessages.BLOCK_ACCESS_LISTS, initialMessage.getData());

    final BlockAccessListsMessage message = BlockAccessListsMessage.readFrom(raw);
    final List<Optional<BlockAccessList>> decoded = new ArrayList<>();
    message.blockAccessLists().forEach(decoded::add);
    assertThat(decoded).containsExactly(Optional.of(expected.get(0)), Optional.of(expected.get(1)));
  }

  @Test
  public void wrapsWithEth71WireShape() {
    // [request-id, [access-lists]]
    final BlockAccessList blockAccessList = new BlockDataGenerator(1).blockAccessList();

    final BlockAccessListsMessage message =
        BlockAccessListsMessage.create(List.of(Optional.of(blockAccessList), Optional.empty()));
    final MessageData wrapped = message.wrapMessageData(BigInteger.valueOf(11));

    final BytesValueRLPOutput expected = new BytesValueRLPOutput();
    expected.startList();
    expected.writeBigIntegerScalar(BigInteger.valueOf(11));
    expected.startList();
    BlockAccessListEncoder.encode(blockAccessList, expected);
    expected.writeBytes(Bytes.EMPTY);
    expected.endList();
    expected.endList();

    assertThat(wrapped.getData()).isEqualTo(expected.encoded());
  }

  @Test
  public void roundTripWithNoBlockAccessLists() {
    final BlockAccessListsMessage initialMessage = BlockAccessListsMessage.create(List.of());
    final RawMessage raw =
        new RawMessage(EthProtocolMessages.BLOCK_ACCESS_LISTS, initialMessage.getData());

    final BlockAccessListsMessage message = BlockAccessListsMessage.readFrom(raw);
    assertThat(message.blockAccessLists()).isEmpty();
  }
}
