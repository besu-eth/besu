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
import java.util.Objects;

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

  public static NewBlockHashesMessage create(
      final Iterable<NewBlockHashesMessage.NewBlockHash> hashes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    for (final NewBlockHashesMessage.NewBlockHash hash : hashes) {
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

  public Iterator<NewBlockHashesMessage.NewBlockHash> getNewHashes() {
    return new BytesValueRLPInput(data, false)
        .readList(
            rlpInput -> {
              rlpInput.enterList();
              final NewBlockHashesMessage.NewBlockHash res =
                  new NewBlockHashesMessage.NewBlockHash(
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

  public static final class NewBlockHash {

    private final Hash hash;

    private final long number;

    public NewBlockHash(final Hash hash, final long number) {
      this.hash = hash;
      this.number = number;
    }

    public long number() {
      return number;
    }

    public Hash hash() {
      return hash;
    }

    @Override
    public String toString() {
      return number() + " (" + hash() + ")";
    }

    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      }
      if (!(that instanceof NewBlockHashesMessage.NewBlockHash other)) {
        return false;
      }
      return other.hash.equals(hash) && other.number == number;
    }

    @Override
    public int hashCode() {
      return Objects.hash(hash, number);
    }
  }
}
