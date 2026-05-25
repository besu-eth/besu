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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;

public final class GetAccountRangeMessage extends AbstractSnapMessageData {

  public GetAccountRangeMessage(final Bytes data) {
    super(data);
  }

  public static GetAccountRangeMessage readFrom(final MessageData message) {
    if (message instanceof GetAccountRangeMessage) {
      return (GetAccountRangeMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_ACCOUNT_RANGE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetAccountRangeMessage.", code));
    }
    return new GetAccountRangeMessage(message.getData());
  }

  public static GetAccountRangeMessage create(
      final Hash worldStateRootHash, final Bytes32 startKeyHash, final Bytes32 endKeyHash) {
    return create(worldStateRootHash, startKeyHash, endKeyHash, SIZE_REQUEST);
  }

  @VisibleForTesting
  public static GetAccountRangeMessage create(
      final Hash worldStateRootHash,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash,
      final BigInteger sizeRequest) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(worldStateRootHash.getBytes());
    tmp.writeBytes(startKeyHash);
    tmp.writeBytes(endKeyHash);
    tmp.writeBigIntegerScalar(sizeRequest);
    tmp.endList();
    return new GetAccountRangeMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.GET_ACCOUNT_RANGE;
  }

  public Range range(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();
    final Hash worldStateRootHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    final ImmutableRange range =
        ImmutableRange.builder()
            .worldStateRootHash(getRootHash().orElse(worldStateRootHash))
            .startKeyHash(Hash.wrap(Bytes32.wrap(input.readBytes32())))
            .endKeyHash(Hash.wrap(Bytes32.wrap(input.readBytes32())))
            .responseBytes(input.readBigIntegerScalar())
            .build();
    input.leaveList();
    return range;
  }

  @Value.Immutable
  public interface Range {

    Optional<BigInteger> requestId();

    Hash worldStateRootHash();

    Hash startKeyHash();

    Hash endKeyHash();

    BigInteger responseBytes();
  }
}
