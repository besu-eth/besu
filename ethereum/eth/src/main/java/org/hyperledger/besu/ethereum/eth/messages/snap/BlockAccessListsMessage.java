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

import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListDecoder;
import org.hyperledger.besu.ethereum.core.encoding.BlockAccessListEncoder;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractSnapMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * snap/2 BlockAccessLists (0x09) per EIP-8189.
 *
 * <p>Wire format: {@code [request-id: P, [block-access-list₁, block-access-list₂, ...]]}.
 *
 * <p>Unavailable BALs are encoded as the RLP empty string ({@code 0x80}) at their positional
 * index, not as an empty list.
 */
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
    final BytesValueRLPOutput output = new BytesValueRLPOutput();
    output.startList();
    requestId.ifPresent(output::writeBigIntegerScalar);
    output.startList();
    for (final BlockAccessList bal : blockAccessLists) {
      if (bal == null || bal.isEmpty()) {
        // EIP-8189: unavailable BALs are encoded as the RLP empty string (0x80).
        output.writeBytes(Bytes.EMPTY);
      } else {
        BlockAccessListEncoder.encode(bal, output);
      }
    }
    output.endList();
    output.endList();
    return new BlockAccessListsMessage(output.encoded());
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
    return () ->
        new Iterator<>() {
          private final RLPInput input = new BytesValueRLPInput(data, false);
          private boolean initialized = false;

          private void ensureInitialized() {
            if (!initialized) {
              input.enterList();
              if (withRequestId) {
                input.skipNext();
              }
              input.enterList();
              initialized = true;
            }
          }

          @Override
          public boolean hasNext() {
            ensureInitialized();
            return !input.isEndOfCurrentList();
          }

          @Override
          public BlockAccessList next() {
            ensureInitialized();
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            // EIP-8189: a positional entry is either a BAL (RLP list) or the empty-string
            // sentinel (0x80) for "unavailable". The sentinel decodes to an empty BAL.
            if (input.nextIsList()) {
              return BlockAccessListDecoder.decode(input.readAsRlp());
            }
            final Bytes raw = input.readBytes();
            if (!raw.isEmpty()) {
              throw new IllegalStateException(
                  "Unexpected non-list, non-empty-string entry in BlockAccessLists response");
            }
            return new BlockAccessList(List.of());
          }
        };
  }
}
