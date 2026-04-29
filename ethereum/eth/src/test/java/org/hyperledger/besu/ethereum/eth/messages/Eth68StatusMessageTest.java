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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Difficulty;
import org.hyperledger.besu.ethereum.eth.EthProtocol;
import org.hyperledger.besu.ethereum.eth.EthProtocolVersion;
import org.hyperledger.besu.ethereum.forkid.ForkId;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.Random;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class Eth68StatusMessageTest {

  private final int version = EthProtocol.ETH68.getVersion();
  private final BigInteger networkId = BigInteger.ONE;
  private final Difficulty td = Difficulty.of(1000L);
  private final Hash bestHash = randHash(1L);
  private final Hash genesisHash = randHash(2L);
  private final ForkId forkId = new ForkId(Bytes.fromHexString("0xa00bc334"), 0L);
  private final StatusMessage.Builder builder =
      StatusMessage.builder()
          .protocolVersion(version)
          .networkId(networkId)
          .totalDifficulty(td)
          .bestHash(bestHash)
          .genesisHash(genesisHash)
          .forkId(forkId);

  @Test
  public void getters() {
    final StatusMessage msg = builder.build();

    assertThat(msg.protocolVersion()).isEqualTo(version);
    assertThat(msg.networkId()).isEqualTo(networkId);
    assertThat(msg.totalDifficulty().get()).isEqualTo(td);
    assertThat(msg.bestHash()).isEqualTo(bestHash);
    assertThat(msg.genesisHash()).isEqualTo(genesisHash.getBytes());
    assertThat(msg.forkId()).isEqualTo(forkId);
  }

  @Test
  public void serializeDeserialize() {
    final MessageData msg = builder.build();

    final StatusMessage copy = StatusMessage.create(msg.getData());

    assertThat(copy.protocolVersion()).isEqualTo(version);
    assertThat(copy.networkId()).isEqualTo(networkId);
    assertThat(copy.totalDifficulty().get()).isEqualTo(td);
    assertThat(copy.bestHash()).isEqualTo(bestHash);
    assertThat(copy.genesisHash()).isEqualTo(genesisHash.getBytes());
    assertThat(copy.forkId()).isEqualTo(forkId);
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
  public void shouldNotHaveBlockRangeWhenEth68() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder
                    .protocolVersion(EthProtocolVersion.V68)
                    .blockRange(new StatusMessage.BlockRange(0L, 10))
                    .build());
    assertThat(exception.getMessage())
        .contains("blockRange is only supported for protocol version >= 69");
  }

  @Test
  public void shouldHaveBlockRangeWhenEth69() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.protocolVersion(EthProtocolVersion.V69).blockRange(null).build());
    assertThat(exception.getMessage())
        .contains("blockRange must be present for protocol version >= 69");
  }

  @Test
  public void shouldHaveTotalDifficultWhenEth68() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.protocolVersion(EthProtocolVersion.V68).totalDifficulty(null).build());
    assertThat(exception.getMessage())
        .contains("totalDifficulty must be present for protocol version <= 68");
  }

  @Test
  public void shouldNotHaveTotalDifficultWhenEth69() {
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder
                    .protocolVersion(EthProtocolVersion.V69)
                    .blockRange(new StatusMessage.BlockRange(0L, 10L))
                    .totalDifficulty(Difficulty.ZERO)
                    .build());
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
    assertThrows(
        RLPException.class,
        () -> StatusMessage.create(out.encoded()).protocolVersion(),
        "StatusMessage should not be able to deserialize invalid data");
  }

  @Test
  public void shouldRejectEth69LayoutWithEth68Version() {
    // Symmetric check: eth/69+ layout (no TD, with blockRange) but version=68 must be rejected.
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();
    out.writeIntScalar(68);
    out.writeBigIntegerScalar(networkId);
    out.writeBytes(genesisHash.getBytes());
    forkId.writeTo(out);
    out.writeLongScalar(0L);
    out.writeLongScalar(123L);
    out.writeBytes(bestHash.getBytes());
    out.endList();
    final RLPException ex =
        assertThrows(
            RLPException.class, () -> StatusMessage.create(out.encoded()).protocolVersion());
    assertThat(ex.getMessage()).contains("version must be >= 69");
  }
}
