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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.apache.tuweni.bytes.Bytes;

public final class NewBlockHashesMessage extends AbstractMessageData {

  public static NewBlockHashesMessage readFrom(final MessageData message) {
    if (message instanceof NewBlockHashesMessage newBlockHashesMessage) {
      return newBlockHashesMessage;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.NEW_BLOCK_HASHES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a NewBlockHashesMessage.", code));
    }
    return new NewBlockHashesMessage(message.getData());
  }

  public static NewBlockHashesMessage create(final Iterable<BlockAnnouncement> hashes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    for (final BlockAnnouncement hash : hashes) {
      tmp.startList();
      tmp.writeBytes(hash.hash().getBytes());
      tmp.writeLongScalar(hash.number());
      tmp.endList();
    }
    tmp.endList();
    return new NewBlockHashesMessage(tmp.encoded());
  }

  private NewBlockHashesMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.NEW_BLOCK_HASHES;
  }

  public Iterator<BlockAnnouncement> getNewHashes() {
    return new BytesValueRLPInput(data, false)
        .readList(
            rlpInput -> {
              rlpInput.enterList();
              final BlockAnnouncement res =
                  new BlockAnnouncement(
                      Hash.wrap(rlpInput.readBytes32()), rlpInput.readLongScalar());
              rlpInput.leaveList();
              return res;
            })
        .iterator();
  }

  @Override
  public String toStringDecoded() {
    return Iterators.toString(getNewHashes());
  }

  public record BlockAnnouncement(Hash hash, long number) {

    @Override
    public String toString() {
      return number() + " (" + hash() + ")";
    }
  }
}
