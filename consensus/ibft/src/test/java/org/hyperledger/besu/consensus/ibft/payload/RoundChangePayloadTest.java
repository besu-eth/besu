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
package org.hyperledger.besu.consensus.ibft.payload;

import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

public class RoundChangePayloadTest {

  private static final ConsensusRoundIdentifier ROUND_IDENTIFIER =
      new ConsensusRoundIdentifier(0x1234567890ABCDEFL, 0xFEDCBA98);
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final SECPSignature SIGNATURE =
      SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);

  @Test
  public void roundTripRlpWithNoPreparedCertificate() {
    final RoundChangePayload roundChangePayload = new RoundChangePayload(ROUND_IDENTIFIER, empty());
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    roundChangePayload.writeTo(rlpOut);

    final RLPInput rlpInput = RLP.input(rlpOut.encoded());
    RoundChangePayload actualRoundChangePayload = RoundChangePayload.readFrom(rlpInput);
    assertThat(actualRoundChangePayload.getRoundIdentifier()).isEqualTo(ROUND_IDENTIFIER);
    assertThat(actualRoundChangePayload.getPreparedCertificate()).isEqualTo(Optional.empty());
    assertThat(actualRoundChangePayload.getMessageType()).isEqualTo(IbftV2.ROUND_CHANGE);
  }

  @Test
  public void roundTripRlpWithEmptyPreparedCertificate() {
    final SignedData<ProposalPayload> signedProposal = signedProposal();

    final PreparedCertificate preparedCertificate =
        new PreparedCertificate(signedProposal, Collections.emptyList());

    final RoundChangePayload roundChangePayload =
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.of(preparedCertificate));
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    roundChangePayload.writeTo(rlpOut);

    final RLPInput rlpInput = RLP.input(rlpOut.encoded());
    RoundChangePayload actualRoundChangePayload = RoundChangePayload.readFrom(rlpInput);
    assertThat(actualRoundChangePayload.getRoundIdentifier()).isEqualTo(ROUND_IDENTIFIER);
    assertThat(actualRoundChangePayload.getPreparedCertificate())
        .isEqualTo(Optional.of(preparedCertificate));
    assertThat(actualRoundChangePayload.getMessageType()).isEqualTo(IbftV2.ROUND_CHANGE);
  }

  @Test
  public void roundTripRlpWithPreparedCertificate() {
    final SignedData<ProposalPayload> signedProposal = signedProposal();

    final PreparePayload preparePayload =
        new PreparePayload(ROUND_IDENTIFIER, Hash.fromHexStringLenient("0x8523ba6e7c5f59ae87"));
    final SignedData<PreparePayload> signedPrepare =
        PayloadDeserializers.from(preparePayload, SIGNATURE);
    final PreparedCertificate preparedCert =
        new PreparedCertificate(signedProposal, Lists.newArrayList(signedPrepare));

    final RoundChangePayload roundChangePayload =
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.of(preparedCert));
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    roundChangePayload.writeTo(rlpOut);

    final RLPInput rlpInput = RLP.input(rlpOut.encoded());
    RoundChangePayload actualRoundChangePayload = RoundChangePayload.readFrom(rlpInput);
    assertThat(actualRoundChangePayload.getRoundIdentifier()).isEqualTo(ROUND_IDENTIFIER);
    assertThat(actualRoundChangePayload.getPreparedCertificate())
        .isEqualTo(Optional.of(preparedCert));
    assertThat(actualRoundChangePayload.getMessageType()).isEqualTo(IbftV2.ROUND_CHANGE);
  }

  private SignedData<ProposalPayload> signedProposal() {
    final Block block =
        ProposedBlockHelpers.createProposalBlock(
            singletonList(AddressHelpers.ofValue(1)), ROUND_IDENTIFIER);
    final ProposalPayload proposalPayload = new ProposalPayload(ROUND_IDENTIFIER, block.getHash());
    final SECPSignature signature =
        SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);
    return PayloadDeserializers.from(proposalPayload, signature);
  }
}
