/*
 * Copyright ConsenSys AG.
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

import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.SyncBlockBody;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

public final class BlockBodiesMessage extends AbstractMessageData {

  public static BlockBodiesMessage readFrom(final MessageData message) {
    if (message instanceof BlockBodiesMessage blockBodiesMessage) {
      return blockBodiesMessage;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.BLOCK_BODIES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a BlockBodiesMessage.", code));
    }
    return new BlockBodiesMessage(message.getData());
  }

  public static BlockBodiesMessage create(final Iterable<BlockBody> bodies) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.writeList(bodies, BlockBody::writeWrappedBodyTo);
    return new BlockBodiesMessage(tmp.encoded());
  }

  /**
   * Create a message with raw, already encoded body data. No checks are performed to validate the
   * rlp-encoded data.
   *
   * @param data An rlp-encoded list of block bodies
   * @return A new BlockBodiesMessage
   */
  public static BlockBodiesMessage createUnsafe(final Bytes data) {
    return new BlockBodiesMessage(data);
  }

  private BlockBodiesMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.BLOCK_BODIES;
  }

  public List<BlockBody> bodies(final ProtocolSchedule protocolSchedule) {
    final BlockHeaderFunctions blockHeaderFunctions =
        ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
    return new BytesValueRLPInput(data, false)
        .readList(rlp -> BlockBody.readWrappedBodyFrom(rlp, blockHeaderFunctions, true));
  }

  public List<SyncBlockBody> syncBodies(final ProtocolSchedule protocolSchedule) {
    return new BytesValueRLPInput(data, false)
        .readList(rlp -> SyncBlockBody.readWrappedBodyFrom(rlp, true, protocolSchedule));
  }
}
