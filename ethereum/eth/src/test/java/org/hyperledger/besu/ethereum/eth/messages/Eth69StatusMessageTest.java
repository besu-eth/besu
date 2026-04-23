/*
 * Copyright contributors to Besu.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class Eth69StatusMessageTest {

  private final int version = EthProtocol.ETH69.getVersion();
  private final BigInteger networkId = BigInteger.ONE;
  private final Hash bestHash = randHash(1L);
  private final Hash genesisHash = randHash(2L);
  private final ForkId forkId = new ForkId(Bytes.fromHexString("0xa00bc334"), 0L);
  private final StatusMessage.BlockRange blockRange = new StatusMessage.BlockRange(0, 123);
  private final StatusMessage.Builder builder =
      StatusMessage.builder()
          .protocolVersion(version)
          .networkId(networkId)
          .bestHash(bestHash)
          .genesisHash(genesisHash)
          .forkId(forkId)
          .blockRange(blockRange);

  @Test
  public void getters() {
    final StatusMessage msg = builder.build();

    assertThat(msg.protocolVersion()).isEqualTo(version);
    assertThat(msg.networkId()).isEqualTo(networkId);
    assertThat(msg.bestHash()).isEqualTo(bestHash);
    assertThat(msg.genesisHash()).isEqualTo(genesisHash.getBytes());
    assertThat(msg.forkId()).isEqualTo(forkId);
    assertThat(msg.blockRange().get()).isEqualTo(blockRange);
  }

  @Test
  public void serializeDeserialize() {
    final MessageData msg = builder.build();

    final StatusMessage copy = StatusMessage.create(msg.getData());

    assertThat(copy.protocolVersion()).isEqualTo(version);
    assertThat(copy.networkId()).isEqualTo(networkId);
    assertThat(copy.bestHash()).isEqualTo(bestHash);
    assertThat(copy.genesisHash()).isEqualTo(genesisHash.getBytes());
    assertThat(copy.forkId()).isEqualTo(forkId);
    assertThat(copy.blockRange().get()).isEqualTo(blockRange);
  }

  @Test
  public void toStringDecodedHasExpectedInfo() {
    final MessageData msg = builder.build();

    final StatusMessage copy = StatusMessage.create(msg.getData());
    final String copyToStringDecoded = copy.toStringDecoded();

    assertThat(copyToStringDecoded).contains("bestHash=" + bestHash);
    assertThat(copyToStringDecoded).contains("genesisHash=" + genesisHash);
  }

  @Test
  public void shouldNotHaveTotalDifficultWhenEth69() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> builder.totalDifficulty(Difficulty.ZERO).build());
    assertThat(exception.getMessage())
        .contains("totalDifficulty must be not present for protocol version >= 69");
  }

  private Hash randHash(final long seed) {
    final Random random = new Random(seed);
    final byte[] bytes = new byte[32];
    random.nextBytes(bytes);
    return Hash.wrap(Bytes32.wrap(bytes));
  }

  @Test
  public void shouldNotHaveTotalDifficultWhen69FromRawInput() {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(69);
    out.writeBigIntegerScalar(networkId);
    out.writeUInt256Scalar(Difficulty.of(1000L));
    out.writeBytes(bestHash.getBytes());
    out.writeBytes(genesisHash.getBytes());
    forkId.writeTo(out);
    out.endList();
    // The decoder must surface this as an RLPException so the eth protocol handler's
    // try/catch for RLPException performs a clean SUBPROTOCOL_TRIGGERED_UNPARSABLE_STATUS
    // disconnect instead of letting an IllegalArgumentException escape the message loop.
    final RLPException ex =
        assertThrows(
            RLPException.class,
            () -> StatusMessage.create(out.encoded()).protocolVersion(),
            "StatusMessage should not be able to deserialize invalid data");
    assertThat(ex.getMessage()).contains("version must be <= 68");
  }

  @Test
  public void shouldRejectEth68LayoutFromProdBesuV26Dot2Develop73d07f9() {
    // Captured verbatim from bal-devnet-3 on 2026-04-15 from peer
    // besu/v26.2-develop-73d07f9, which advertises eth/69 in Hello but encodes Status
    // with the eth/68 layout (version=69, totalDifficulty present).
    // Nimbus rejects this; Besu must too, via RLPException.
    final Bytes bytes =
        Bytes.fromHexString(
            "0xf858458206bf8a0c70d815d562d3cfa955a035a6d5ca044bd77ca9169d2f77d0cb59ce19541a59a66097625620e543349c4aa0d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3c684fc9bfe8f80");
    final RLPException ex =
        assertThrows(
            RLPException.class,
            () -> StatusMessage.create(bytes).protocolVersion(),
            "Real-world malformed Status from a broken eth/69 encoder must be rejected");
    assertThat(ex.getMessage()).contains("version must be <= 68");
  }
}
