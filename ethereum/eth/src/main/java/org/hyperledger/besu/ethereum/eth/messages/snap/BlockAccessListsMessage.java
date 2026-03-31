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

import org.hyperledger.besu.ethereum.eth.messages.BlockAccessListsMessageData;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public final class BlockAccessListsMessage extends AbstractSnapMessageData {

  public BlockAccessListsMessage(final Bytes data) {
    super(data);
  }

  public static BlockAccessListsMessage readFrom(final MessageData message) {
    if (message instanceof BlockAccessListsMessage) {
      return (BlockAccessListsMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV2.BLOCK_ACCESS_LISTS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a BlockAccessListsMessage.", code));
    }
    return new BlockAccessListsMessage(message.getData());
  }

  public static BlockAccessListsMessage create(final Iterable<BlockAccessList> blockAccessLists) {
    return create(Optional.empty(), blockAccessLists);
  }

  public static BlockAccessListsMessage create(
      final Optional<BigInteger> requestId, final Iterable<BlockAccessList> blockAccessLists) {
    return new BlockAccessListsMessage(
        BlockAccessListsMessageData.encode(requestId, blockAccessLists));
  }

  /**
   * Create a message with raw, already encoded data. No checks are performed to validate the
   * rlp-encoded data.
   *
   * @param data An rlp-encoded list of block access lists
   * @return A new BlockAccessListsMessage
   */
  public static BlockAccessListsMessage createUnsafe(final Bytes data) {
    return new BlockAccessListsMessage(data);
  }

  @Override
  protected Bytes wrap(final BigInteger requestId) {
    return create(Optional.of(requestId), blockAccessLists(false)).getData();
  }

  @Override
  public int getCode() {
    return SnapV2.BLOCK_ACCESS_LISTS;
  }

  public Iterable<BlockAccessList> blockAccessLists(final boolean withRequestId) {
    return BlockAccessListsMessageData.decode(data, withRequestId);
  }
}
