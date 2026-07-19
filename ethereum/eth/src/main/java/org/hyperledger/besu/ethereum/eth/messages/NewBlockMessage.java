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

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ScheduleBasedBlockHeaderFunctions;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

public class NewBlockMessage extends AbstractMessageData {

  private static final int MESSAGE_CODE = EthProtocolMessages.NEW_BLOCK;

  private NewBlockMessageData messageFields = null;

  private NewBlockMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  public static NewBlockMessage create(
      final Block block, final Difficulty totalDifficulty, final int maxMessageSize)
      throws IllegalArgumentException {
    final NewBlockMessageData msgData = new NewBlockMessageData(block, totalDifficulty);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    msgData.writeTo(out);
    final Bytes data = out.encoded();
    if (data.size() > maxMessageSize) {
      throw new IllegalArgumentException(
          String.format(
              "Block message size %d bytes is larger than allowed message size %d bytes",
              data.size(), maxMessageSize));
    }
    return new NewBlockMessage(data);
  }

  public static NewBlockMessage readFrom(final MessageData message) {
    if (message instanceof NewBlockMessage newBlockMessage) {
      return newBlockMessage;
    }
    final int code = message.getCode();
    if (code != NewBlockMessage.MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a NewBlockMessage.", code));
    }
    return new NewBlockMessage(message.getData());
  }

  public Block block(final ProtocolSchedule protocolSchedule) {
    return messageFields(protocolSchedule).block();
  }

  public Difficulty totalDifficulty(final ProtocolSchedule protocolSchedule) {
    return messageFields(protocolSchedule).totalDifficulty();
  }

  private NewBlockMessageData messageFields(final ProtocolSchedule protocolSchedule) {
    if (messageFields == null) {
      final RLPInput input = RLP.input(data);
      messageFields = NewBlockMessageData.readFrom(input, protocolSchedule);
    }
    return messageFields;
  }

  public static class NewBlockMessageData {

    private final Block block;
    private final Difficulty totalDifficulty;

    public NewBlockMessageData(final Block block, final Difficulty totalDifficulty) {
      this.block = block;
      this.totalDifficulty = totalDifficulty;
    }

    public Block block() {
      return block;
    }

    public Difficulty totalDifficulty() {
      return totalDifficulty;
    }

    public void writeTo(final RLPOutput out) {
      out.startList();
      block.writeTo(out);
      out.writeUInt256Scalar(totalDifficulty);
      out.endList();
    }

    public static NewBlockMessageData readFrom(
        final RLPInput in, final ProtocolSchedule protocolSchedule) {
      final BlockHeaderFunctions blockHeaderFunctions =
          ScheduleBasedBlockHeaderFunctions.create(protocolSchedule);
      in.enterList();
      final Block block = Block.readFrom(in, blockHeaderFunctions);
      final UInt256 totaldifficulty = in.readUInt256Scalar();
      return new NewBlockMessageData(block, Difficulty.of(totaldifficulty));
    }
  }
}
