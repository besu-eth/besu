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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.immutables.value.Value;

/**
 * snap/2 GetBlockAccessLists (0x08) per EIP-8189.
 *
 * <p>Wire format: {@code [request-id: P, [blockhash₁: B_32, blockhash₂: B_32, ...], bytes: P]}.
 */
public final class GetBlockAccessListsMessage extends AbstractSnapMessageData {

  /** EIP-8189 recommended soft response limit for BlockAccessLists (2 MiB). */
  public static final BigInteger DEFAULT_RESPONSE_BYTES = BigInteger.valueOf(2L * 1024 * 1024);

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

  public static GetBlockAccessListsMessage create(final List<Hash> blockHashes) {
    return create(Optional.empty(), blockHashes, DEFAULT_RESPONSE_BYTES);
  }

  public static GetBlockAccessListsMessage create(
      final Optional<BigInteger> requestId,
      final List<Hash> blockHashes,
      final BigInteger responseBytes) {
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    tmp.startList();
    requestId.ifPresent(tmp::writeBigIntegerScalar);
    tmp.writeList(blockHashes, (hash, rlpOutput) -> rlpOutput.writeBytes(hash.getBytes()));
    tmp.writeBigIntegerScalar(responseBytes);
    tmp.endList();
    return new GetBlockAccessListsMessage(tmp.encoded());
  }

  @Override
  protected Bytes wrap(final BigInteger requestId) {
    final BlockHashes decoded = blockHashes(false);
    return create(Optional.of(requestId), decoded.hashes(), decoded.responseBytes()).getData();
  }

  @Override
  public int getCode() {
    return SnapV2.GET_BLOCK_ACCESS_LISTS;
  }

  public BlockHashes blockHashes(final boolean withRequestId) {
    final List<Hash> hashes = new ArrayList<>();
    final RLPInput input = new BytesValueRLPInput(data, false);
    input.enterList();
    if (withRequestId) {
      input.skipNext();
    }
    input.enterList();
    while (!input.isEndOfCurrentList()) {
      hashes.add(Hash.wrap(input.readBytes32()));
    }
    input.leaveList();
    final BigInteger responseBytes = input.readBigIntegerScalar();
    input.leaveList();
    return ImmutableBlockHashes.builder().hashes(hashes).responseBytes(responseBytes).build();
  }

  @Value.Immutable
  public interface BlockHashes {

    List<Hash> hashes();

    BigInteger responseBytes();
  }
}
