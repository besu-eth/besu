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

import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.AbstractMap;
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

  /**
   * Wraps this message by prepending a requestId to the existing RLP-encoded data. Uses zero-copy
   * splicing: the payload bytes are shared via {@link Bytes#wrap(Bytes...)} rather than copied.
   *
   * <p>Original: [field1, field2, ...] Wrapped: [requestId, [field1, field2, ...]]
   */
  default MessageData wrapMessageData(final BigInteger requestId) {
    final Bytes data = getData();
    final Bytes reqIdBytes = RLP.encode(out -> out.writeBigIntegerScalar(requestId));
    return new RawMessage(
        getCode(), Bytes.wrap(RLP.listHeader(reqIdBytes.size() + data.size()), reqIdBytes, data));
  }

  /**
   * Unwraps a requestId-prefixed message, returning the requestId and the original message. Uses
   * zero-copy slicing rather than copying.
   *
   * <p>Wrapped: [requestId, [field1, field2, ...]] Unwrapped: requestId + [field1, field2, ...]
   */
  default Map.Entry<BigInteger, MessageData> unwrapMessageData() {
    final Bytes data = getData();
    final int listHeaderSize = RLP.listHeaderSize(data);
    final Bytes content = data.slice(listHeaderSize);
    // Read the requestId value
    final RLPInput messageDataRLP = RLP.input(data);
    messageDataRLP.enterList();
    final BigInteger requestId = messageDataRLP.readBigIntegerScalar();
    messageDataRLP.leaveListLenient();
    // Slice past the requestId element to get the inner message
    final int reqIdSize = RLP.calculateSize(content);
    final Bytes remaining = content.slice(reqIdSize);
    return new AbstractMap.SimpleImmutableEntry<>(requestId, new RawMessage(getCode(), remaining));
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
