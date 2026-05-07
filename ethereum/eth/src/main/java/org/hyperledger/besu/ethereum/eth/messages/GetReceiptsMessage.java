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
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tuweni.bytes.Bytes;

public class GetReceiptsMessage extends AbstractMessageData {

  protected GetReceiptsMessage(final Bytes data) {
    super(data);
  }

  public static GetReceiptsMessage readFrom(final MessageData message) {
    if (message instanceof GetReceiptsMessage) {
      return (GetReceiptsMessage) message;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.GET_RECEIPTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetReceipts.", code));
    }
    return new GetReceiptsMessage(message.getData());
  }

  public static GetReceiptsMessage create(final List<Hash> blockHashes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    blockHashes.forEach(hash -> tmp.writeBytes(hash.getBytes()));
    tmp.endList();
    return new GetReceiptsMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.GET_RECEIPTS;
  }

  public Iterable<Hash> blockHashes() {
    return () ->
        new Iterator<>() {
          private final RLPInput input = new BytesValueRLPInput(data, false);

          {
            input.enterList();
          }

          @Override
          public boolean hasNext() {
            return !input.isEndOfCurrentList();
          }

          @Override
          public Hash next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            return Hash.wrap(input.readBytes32());
          }
        };
  }
}
