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
package org.hyperledger.besu.ethereum.eth.messages;

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.AbstractMessageData;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Optional;
import java.util.OptionalLong;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** PV62 GetBlockHeaders Message. */
public final class GetBlockHeadersMessage extends AbstractMessageData {

  private GetBlockHeadersData getBlockHeadersData = null;

  public static GetBlockHeadersMessage readFrom(final MessageData message) {
    if (message instanceof GetBlockHeadersMessage getBlockHeadersMessage) {
      return getBlockHeadersMessage;
    }
    final int code = message.getCode();
    if (code != EthProtocolMessages.GET_BLOCK_HEADERS) {
      throw new IllegalArgumentException(
          String.format("Message has code %d and thus is not a GetBlockHeadersMessage.", code));
    }
    return new GetBlockHeadersMessage(message.getData());
  }

  public static GetBlockHeadersMessage create(
      final long blockNum, final int maxHeaders, final int skip, final boolean reverse) {
    final GetBlockHeadersData getBlockHeadersData =
        GetBlockHeadersData.create(blockNum, maxHeaders, skip, reverse);
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    getBlockHeadersData.writeTo(tmp);
    return new GetBlockHeadersMessage(tmp.encoded());
  }

  public static GetBlockHeadersMessage create(
      final Hash hash, final int maxHeaders, final int skip, final boolean reverse) {
    final GetBlockHeadersData getBlockHeadersData =
        GetBlockHeadersData.create(hash, maxHeaders, skip, reverse);
    final BytesValueRLPOutput tmp = new BytesValueRLPOutput();
    getBlockHeadersData.writeTo(tmp);
    return new GetBlockHeadersMessage(tmp.encoded());
  }

  private GetBlockHeadersMessage(final Bytes data) {
    super(data);
  }

  @Override
  public int getCode() {
    return EthProtocolMessages.GET_BLOCK_HEADERS;
  }

  /**
   * Returns the block number that the message requests or {@link OptionalLong#empty()} if the
   * request specifies a block hash.
   *
   * @return Block Number Requested or {@link OptionalLong#empty()}
   */
  public OptionalLong blockNumber() {
    return getBlockHeadersData().blockNumber;
  }

  /**
   * Returns the block hash that the message requests or {@link Optional#empty()} if the request
   * specifies a block number.
   *
   * @return Block Hash Requested or {@link Optional#empty()}
   */
  public Optional<Hash> hash() {
    return getBlockHeadersData().blockHash;
  }

  public int maxHeaders() {
    return getBlockHeadersData().maxHeaders;
  }

  public int skip() {
    return getBlockHeadersData().skip;
  }

  public boolean reverse() {
    return getBlockHeadersData().reverse;
  }

  private GetBlockHeadersData getBlockHeadersData() {
    if (getBlockHeadersData == null) {
      getBlockHeadersData = GetBlockHeadersData.readFrom(RLP.input(data));
    }
    return getBlockHeadersData;
  }

  private static class GetBlockHeadersData {
    private final Optional<Hash> blockHash;
    private final OptionalLong blockNumber;
    private final int maxHeaders;
    private final int skip;
    private final boolean reverse;

    private GetBlockHeadersData(
        final Optional<Hash> blockHash,
        final OptionalLong blockNumber,
        final int maxHeaders,
        final int skip,
        final boolean reverse) {
      checkArgument(
          validateBlockHashAndNumber(blockHash, blockNumber),
          "Either blockHash or blockNumber should be non-empty");
      this.blockHash = blockHash;
      this.blockNumber = blockNumber;
      this.maxHeaders = maxHeaders;
      this.skip = skip;
      this.reverse = reverse;
    }

    private static boolean validateBlockHashAndNumber(
        final Optional<Hash> blockHash, final OptionalLong blockNumber) {
      return (blockHash.isPresent() || blockNumber.isPresent())
          && !(blockHash.isPresent() && blockNumber.isPresent());
    }

    public static GetBlockHeadersData readFrom(final RLPInput input) {
      input.enterList();

      final Optional<Hash> blockHash;
      final OptionalLong blockNumber;
      if (input.nextSize() == Bytes32.SIZE) {
        blockHash = Optional.of(Hash.wrap(input.readBytes32()));
        blockNumber = OptionalLong.empty();
      } else {
        blockHash = Optional.empty();
        blockNumber = OptionalLong.of(input.readLongScalar());
      }

      final int maxHeaders = input.readIntScalar();
      final int skip = input.readIntScalar();
      final boolean reverse = input.readIntScalar() != 0;

      input.leaveList();

      return new GetBlockHeadersData(blockHash, blockNumber, maxHeaders, skip, reverse);
    }

    public static GetBlockHeadersData create(
        final long blockNum, final int maxHeaders, final int skip, final boolean reverse) {
      return new GetBlockHeadersData(
          Optional.empty(), OptionalLong.of(blockNum), maxHeaders, skip, reverse);
    }

    public static GetBlockHeadersData create(
        final Hash hash, final int maxHeaders, final int skip, final boolean reverse) {
      return new GetBlockHeadersData(
          Optional.of(hash), OptionalLong.empty(), maxHeaders, skip, reverse);
    }

    /**
     * Write an RLP representation.
     *
     * @param out The RLP output to write to
     */
    public void writeTo(final RLPOutput out) {
      out.startList();

      if (blockHash.isPresent()) {
        out.writeBytes(blockHash.get().getBytes());
      } else {
        out.writeLongScalar(blockNumber.getAsLong());
      }
      out.writeIntScalar(maxHeaders);
      out.writeIntScalar(skip);
      out.writeIntScalar(reverse ? 1 : 0);

      out.endList();
    }
  }
}
