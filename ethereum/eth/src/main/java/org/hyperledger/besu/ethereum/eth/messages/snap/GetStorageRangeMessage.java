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
import org.immutables.value.Value;

public final class GetStorageRangeMessage extends AbstractSnapMessageData {

  public GetStorageRangeMessage(final Bytes data) {
    super(data);
  }

  public static GetStorageRangeMessage readFrom(final MessageData message) {
    if (message instanceof GetStorageRangeMessage) {
      return (GetStorageRangeMessage) message;
    }
    final int code = message.getCode();
    if (code != SnapV1.GET_STORAGE_RANGE) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetStorageRangeMessage.", code));
    }
    return new GetStorageRangeMessage(message.getData());
  }

  public static GetStorageRangeMessage create(
      final Hash worldStateRootHash,
      final List<Bytes32> accountHashes,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    tmp.writeBytes(worldStateRootHash.getBytes());
    tmp.writeList(accountHashes, (hash, rlpOutput) -> rlpOutput.writeBytes(hash));
    tmp.writeBytes(startKeyHash);
    tmp.writeBytes(endKeyHash);
    tmp.writeBigIntegerScalar(SIZE_REQUEST);
    tmp.endList();
    return new GetStorageRangeMessage(tmp.encoded());
  }

  @Override
  public int getCode() {
    return SnapV1.GET_STORAGE_RANGE;
  }

  public StorageRange range(final boolean withRequestId) {
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();

    final Hash wireRootHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    // Zero-copy slice of the RLP-encoded account hash list; iterated on demand by accountHashes().
    final Bytes rawAccountHashes = input.readAsRlp().raw();

    final Hash startKeyHash;
    if (input.nextIsNull()) {
      input.skipNext();
      startKeyHash = Hash.ZERO;
    } else {
      startKeyHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    }
    final Hash endKeyHash;
    if (input.nextIsNull()) {
      input.skipNext();
      endKeyHash = Hash.LAST;
    } else {
      endKeyHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    }

    return ImmutableStorageRange.builder()
        .worldStateRootHash(getRootHash().orElse(wireRootHash))
        .rawAccountHashes(rawAccountHashes)
        .startKeyHash(startKeyHash)
        .endKeyHash(endKeyHash)
        .responseBytes(input.readBigIntegerScalar())
        .build();
  }

  @Value.Immutable
  public interface StorageRange {

    Hash worldStateRootHash();

    Bytes rawAccountHashes();

    Hash startKeyHash();

    Hash endKeyHash();

    BigInteger responseBytes();

    /**
     * Lazily iterates the requested account hashes by decoding {@link #rawAccountHashes()} on
     * demand. Each call returns a fresh independent iterator so the snap server can stop early once
     * its response budget is exhausted without materializing the full peer-supplied list.
     */
    default Iterable<Bytes32> accountHashes() {
      return () ->
          new Iterator<>() {
            private final BytesValueRLPInput input =
                new BytesValueRLPInput(rawAccountHashes(), false);
            private Bytes32 peeked = null;
            private boolean exhausted = false;

            {
              input.enterList();
            }

            @Override
            public boolean hasNext() {
              if (peeked != null) return true;
              if (exhausted || input.isEndOfCurrentList()) {
                exhausted = true;
                return false;
              }
              peeked = input.readBytes32();
              return true;
            }

            @Override
            public Bytes32 next() {
              if (!hasNext()) throw new NoSuchElementException();
              final Bytes32 result = peeked;
              peeked = null;
              return result;
            }
          };
    }

    /** Returns {@code true} if the request covers more than one account. */
    default boolean hasMultipleAccountHashes() {
      final var it = accountHashes().iterator();
      if (it.hasNext()) it.next();
      return it.hasNext();
    }
  }
}
