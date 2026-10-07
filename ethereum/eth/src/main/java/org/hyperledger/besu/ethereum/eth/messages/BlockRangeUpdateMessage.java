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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import org.apache.tuweni.bytes.Bytes;

public class BlockRangeUpdateMessage extends AbstractMessageData {

  private static final int MESSAGE_CODE = EthProtocolMessages.BLOCK_RANGE_UPDATE;

  private BlockRangeUpdateMessageData messageFields = null;

  private BlockRangeUpdateMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return MESSAGE_CODE;
  }

  public static BlockRangeUpdateMessage create(
      final long earliestBlockNumber, final long latestBlockNumber, final Hash blockHash)
      throws IllegalArgumentException {
    final BlockRangeUpdateMessageData msgData =
        new BlockRangeUpdateMessageData(earliestBlockNumber, latestBlockNumber, blockHash);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    msgData.writeTo(out);
    final Bytes data = out.encoded();
    return new BlockRangeUpdateMessage(data);
  }

  private BlockRangeUpdateMessageData messageFields() {
    if (messageFields == null) {
      final RLPInput input = RLP.input(data);
      messageFields = BlockRangeUpdateMessageData.readFrom(input);
    }
    return messageFields;
  }

  public long getEarliestBlockNumber() {
    return messageFields().earliestBlockNumber;
  }

  public long getLatestBlockNumber() {
    return messageFields().latestBlockNumber;
  }

  public Hash getBlockHash() {
    return messageFields().blockHash;
  }

  public static BlockRangeUpdateMessage readFrom(final MessageData message) {
    if (message instanceof BlockRangeUpdateMessage blockRangeUpdateMessage) {
      return blockRangeUpdateMessage;
    }
    final int code = message.getCode();
    if (code != BlockRangeUpdateMessage.MESSAGE_CODE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a BlockRangeUpdateMessage.", code));
    }
    return new BlockRangeUpdateMessage(message.getData());
  }

  public record BlockRangeUpdateMessageData(
      long earliestBlockNumber, long latestBlockNumber, Hash blockHash) {

    public void writeTo(final RLPOutput out) {
      out.startList();
      out.writeLongScalar(earliestBlockNumber);
      out.writeLongScalar(latestBlockNumber);
      out.writeBytes(blockHash.getBytes());
      out.endList();
    }

    public static BlockRangeUpdateMessageData readFrom(final RLPInput in) {
      in.enterList();
      final long earliestBlockNumber = in.readLongScalar();
      final long latestBlockNumber = in.readLongScalar();
      final Hash blockHash = Hash.wrap(in.readBytes32());
      in.leaveList();
      return new BlockRangeUpdateMessageData(earliestBlockNumber, latestBlockNumber, blockHash);
    }
  }
}
