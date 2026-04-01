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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.eth.messages.GetBlockAccessListsMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public final class GetBlockAccessListsMessage extends AbstractSnapMessageData {

  public GetBlockAccessListsMessage(final Bytes data) {
    super(data);
  }

  public static GetBlockAccessListsMessage readFrom(final MessageData message) {
    if (message instanceof GetBlockAccessListsMessage) {
      return (GetBlockAccessListsMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV2.GET_BLOCK_ACCESS_LISTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetBlockAccessListsMessage.", code));
    }
    return new GetBlockAccessListsMessage(message.getData());
  }

  public static GetBlockAccessListsMessage create(final Iterable<Hash> blockHashes) {
    return create(Optional.empty(), blockHashes);
  }

  public static GetBlockAccessListsMessage create(
      final Optional<BigInteger> requestId, final Iterable<Hash> blockHashes) {
    return new GetBlockAccessListsMessage(
        GetBlockAccessListsMessageData.encode(requestId, blockHashes));
  }

  @Override
  protected Bytes wrap(final BigInteger requestId) {
    return create(Optional.of(requestId), blockHashes(false)).getData();
  }

  @Override
  public int getCode() {
    return SnapV2.GET_BLOCK_ACCESS_LISTS;
  }

  public Iterable<Hash> blockHashes(final boolean withRequestId) {
    return GetBlockAccessListsMessageData.decode(data, withRequestId);
  }
}
