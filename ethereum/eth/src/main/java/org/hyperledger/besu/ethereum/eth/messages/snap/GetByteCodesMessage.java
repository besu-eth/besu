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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

public final class GetByteCodesMessage extends AbstractSnapMessageData {

  public GetByteCodesMessage(final Bytes data) {
    super(data);
  }

  public static GetByteCodesMessage readFrom(final MessageData message) {
    if (message instanceof GetByteCodesMessage) {
      return (GetByteCodesMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_BYTECODES) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetByteCodesMessage.", code));
    }
    return new GetByteCodesMessage(message.getData());
  }

  public static GetByteCodesMessage create(final List<Bytes32> codeHashes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeList(codeHashes, (hash, rlpOutput) -> rlpOutput.writeBytes(hash));
    tmp.writeBigIntegerScalar(SIZE_REQUEST);
    tmp.endList();
    return new GetByteCodesMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.GET_BYTECODES;
  }

  /**
   * Lazily iterates the requested code hashes, decoding each element from the RLP on demand. This
   * lets the snap server stop iterating once it reaches its per-request lookup limit instead of
   * materializing the full peer-supplied list. Wire layout: {@code [requestId?, [hashes],
   * responseBytes]}.
   *
   * @param withRequestId whether the payload is wrapped with a leading request id
   * @return an on-demand iterable over the requested code hashes
   */
  public Iterable<Bytes32> codeHashes(final boolean withRequestId) {
    return () ->
        new Iterator<>() {
          private final RLPInput input = new BytesValueRLPInput(data, false);
          private Bytes32 peeked = null;
          private boolean exhausted = false;

          {
            input.enterList();
            if (withRequestId) {
              input.skipNext();
            }
            input.enterList();
          }

          @Override
          public boolean hasNext() {
            if (peeked != null) {
              return true;
            }
            if (exhausted || input.isEndOfCurrentList()) {
              exhausted = true;
              return false;
            }
            peeked = input.readBytes32();
            return true;
          }

          @Override
          public Bytes32 next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final Bytes32 result = peeked;
            peeked = null;
            return result;
          }
        };
  }

  /**
   * Reads the trailing response-bytes limit, skipping the (potentially large) hash list.
   *
   * @param withRequestId whether the payload is wrapped with a leading request id
   * @return the requested maximum response size in bytes
   */
  public BigInteger responseBytes(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) {
      input.skipNext();
    }
    input.skipNext();
    final BigInteger responseBytes = input.readBigIntegerScalar();
    input.leaveListLenient();
    return responseBytes;
  }
}
