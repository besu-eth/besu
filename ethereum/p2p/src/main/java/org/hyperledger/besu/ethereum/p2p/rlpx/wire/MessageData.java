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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire;

import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;

/** A P2P Network Message's Data. */
public interface MessageData extends org.hyperledger.besu.datatypes.p2p.MessageData {
  /**
   * Returns the size of the message.
   *
   * @return Number of bytes in this data.
   */
  @Override
  int getSize();

  /**
   * Returns the message's code.
   *
   * @return Message Code
   */
  @Override
  int getCode();

  /**
   * Get the serialized representation for this message
   *
   * @return the serialized representation of this message
   */
  @Override
  Bytes getData();

  default MessageData wrapMessageData(final BigInteger requestId) {
    final BytesValueRLPOutput rlpOutput = new BytesValueRLPOutput();
    rlpOutput.startList();
    rlpOutput.writeBigIntegerScalar(requestId);
    rlpOutput.writeRaw(getData());
    rlpOutput.endList();
    return new RawMessage(getCode(), rlpOutput.encoded());
  }

  default Map.Entry<BigInteger, MessageData> unwrapMessageData() {
    final RLPInput messageDataRLP = RLP.input(getData());
    messageDataRLP.enterList();
    final BigInteger requestId = messageDataRLP.readBigIntegerScalar();
    final var params = new ArrayList<Bytes>();
    while (!messageDataRLP.isEndOfCurrentList()) {
      params.add(messageDataRLP.readAsRlp().raw());
    }
    messageDataRLP.leaveList();
    return new AbstractMap.SimpleImmutableEntry<>(
        requestId, new RawMessage(getCode(), Bytes.concatenate(params)));
  }

  /**
   * Subclasses can implement this method to return a human-readable version of the raw data.
   *
   * @return return a human-readable version of the raw data
   */
  default String toStringDecoded() {
    return "N/A";
  }
}
