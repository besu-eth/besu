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
import java.util.List;
import java.util.Optional;

import kotlin.collections.ArrayDeque;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

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
    return create(Optional.empty(), worldStateRootHash, accountHashes, startKeyHash, endKeyHash);
  }

  public static GetStorageRangeMessage create(
      final Optional<BigInteger> requestId,
      final Hash worldStateRootHash,
      final List<Bytes32> accountHashes,
      final Bytes32 startKeyHash,
      final Bytes32 endKeyHash) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    requestId.ifPresent(tmp::writeBigIntegerScalar);
    tmp.writeBytes(worldStateRootHash.getBytes());
    tmp.writeList(accountHashes, (hash, rlpOutput) -> rlpOutput.writeBytes(hash));
    tmp.writeBytes(startKeyHash);
    tmp.writeBytes(endKeyHash);
    tmp.writeBigIntegerScalar(SIZE_REQUEST);
    tmp.endList();
    return new GetStorageRangeMessage(tmp.encoded());
  }

  @Override
  protected Bytes wrap(final BigInteger requestId) {
    final StorageRange range = range(false);
    return create(
            Optional.of(requestId),
            range.worldStateRootHash(),
            range.hashes(),
            Bytes32.wrap(range.startKeyHash().getBytes()),
            range.endKeyHash() != null ? Bytes32.wrap(range.endKeyHash().getBytes()) : null)
        .getData();
  }

  @Override
  public int getCode() {
    return SnapV1.GET_STORAGE_RANGE;
  }

  public StorageRange range(final boolean withRequestId) {
    final ArrayDeque<Bytes32> hashes = new ArrayDeque<>();
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) input.skipNext();
    final Hash worldStateRootHash = Hash.wrap(Bytes32.wrap(input.readBytes32()));
    final ImmutableStorageRange.Builder range =
        ImmutableStorageRange.builder()
            .worldStateRootHash(getRootHash().orElse(worldStateRootHash));
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      hashes.add(input.readBytes32());
    }
    range.hashes(hashes);
    input.leaveList();

    if (input.nextIsNull()) {
      input.skipNext();
      range.startKeyHash(Hash.ZERO);
    } else {
      range.startKeyHash(Hash.wrap(Bytes32.wrap(input.readBytes32())));
    }
    if (input.nextIsNull()) {
      input.skipNext();
      range.endKeyHash(Hash.LAST);
    } else {
      range.endKeyHash(Hash.wrap(Bytes32.wrap(input.readBytes32())));
    }
    range.responseBytes(input.readBigIntegerScalar());
    input.leaveList();
    return range.build();
  }

  @Value.Immutable
  public interface StorageRange {

    Hash worldStateRootHash();

    ArrayDeque<Bytes32> hashes();

    Hash startKeyHash();

    @Nullable Hash endKeyHash();

    BigInteger responseBytes();
  }
}
