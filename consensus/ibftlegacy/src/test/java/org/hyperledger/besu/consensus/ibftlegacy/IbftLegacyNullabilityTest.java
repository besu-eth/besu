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
package org.hyperledger.besu.consensus.ibftlegacy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class IbftLegacyNullabilityTest {

  private static final IbftExtraDataCodec CODEC = new IbftExtraDataCodec();

  @Test
  void recoverProposerAddressThrowsWhenProposerSealIsMissing() {
    final BlockHeader header = new BlockHeaderTestFixture().number(1).buildHeader();
    final IbftLegacyExtraData extraData =
        new IbftLegacyExtraData(
            Bytes.repeat((byte) 0x00, IbftExtraDataCodec.EXTRA_VANITY_LENGTH),
            List.of(),
            null,
            List.of());

    assertThatThrownBy(() -> IbftBlockHashing.recoverProposerAddress(header, extraData))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Missing proposer seal");
  }

  @Test
  void decodeRawTreatsNullRlpValueAsMissingProposerSeal() {
    final Bytes rawExtraData = createRawExtraData(null);

    final IbftLegacyExtraData decoded = CODEC.decodeRaw(rawExtraData);

    assertThat(decoded.getProposerSeal()).isNull();
  }

  @Test
  void decodeRawTreatsAllZeroProposerSealBytesAsMissingProposerSeal() {
    final Bytes rawExtraData = createRawExtraData(Bytes.repeat((byte) 0x00, 65));

    final IbftLegacyExtraData decoded = CODEC.decodeRaw(rawExtraData);

    assertThat(decoded.getProposerSeal()).isNull();
  }

  private static Bytes createRawExtraData(final Bytes proposerSealBytes) {
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    out.startList();

    out.startList();
    out.endList();

    if (proposerSealBytes == null) {
      out.writeNull();
    } else {
      out.writeBytes(proposerSealBytes);
    }

    out.writeEmptyList();
    out.endList();

    return Bytes.wrap(
        Bytes.repeat((byte) 0x00, IbftExtraDataCodec.EXTRA_VANITY_LENGTH), out.encoded());
  }
}
