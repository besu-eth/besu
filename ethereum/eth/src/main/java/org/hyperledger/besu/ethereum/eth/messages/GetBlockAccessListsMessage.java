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
package org.hyperledger.besu.ethereum.eth.messages;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public final class GetBlockAccessListsMessage extends AbstractMessageData {

  public static GetBlockAccessListsMessage readFrom(final MessageData message) {
    if (message instanceof GetBlockAccessListsMessage) {
      return (GetBlockAccessListsMessage) message;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.GET_BLOCK_ACCESS_LISTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetBlockAccessListsMessage.", code));
    }
    return new GetBlockAccessListsMessage(message.getData());
  }

  public static GetBlockAccessListsMessage create(final Iterable<Hash> blockHashes) {
    return new GetBlockAccessListsMessage(
        GetBlockAccessListsMessageData.encode(Optional.empty(), blockHashes));
  }

  private GetBlockAccessListsMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.GET_BLOCK_ACCESS_LISTS;
  }

  public Iterable<Hash> blockHashes() {
    return GetBlockAccessListsMessageData.decode(data, false);
  }
}
