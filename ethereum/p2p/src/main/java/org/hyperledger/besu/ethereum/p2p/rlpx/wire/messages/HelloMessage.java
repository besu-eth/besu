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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.PeerInfo;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;

import org.apache.tuweni.bytes.Bytes;

public final class HelloMessage extends AbstractMessageData {

  private HelloMessage(final Bytes data) {
    super(data);
  }

  public static HelloMessage create(final PeerInfo peerInfo) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    peerInfo.writeTo(out);

    return new HelloMessage(out.encoded());
  }

  public static HelloMessage readFrom(final MessageData message) {
    if (message instanceof HelloMessage helloMessage) {
      return helloMessage;
    }
    final int code = message.getCode();
    if (code != WireMessageCodes.HELLO) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a HelloMessage.", code));
    }
    return new HelloMessage(message.getData());
  }

  @Override
  public int getCode() {
    return WireMessageCodes.HELLO;
  }

  public PeerInfo getPeerInfo() {
    return PeerInfo.readFrom(RLP.input(data));
  }
}
