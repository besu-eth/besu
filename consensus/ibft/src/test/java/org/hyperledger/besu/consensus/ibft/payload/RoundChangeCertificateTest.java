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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.common.bft.messagewrappers.BftMessage;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

public class RoundChangeCertificateTest {
  private static final ConsensusRoundIdentifier ROUND_IDENTIFIER =
      new ConsensusRoundIdentifier(0x1234567890ABCDEFL, 0xFEDCBA98);

  private SignedData<RoundChangePayload> fakeSignedRoundChange() {
    final SECPSignature sig =
        SignatureAlgorithmFactory.getInstance()
            .createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);
    final RoundChangePayload payload = new RoundChangePayload(ROUND_IDENTIFIER, Optional.empty());
    return PayloadDeserializers.from(payload, sig);
  }

  private SignedData<RoundChangePayload> fakeSignedRoundChangeWithPreparedCert(
      final int prepareCount) {
    final SECPSignature sig =
        SignatureAlgorithmFactory.getInstance()
            .createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);
    final SignedData<ProposalPayload> signedProposal =
        PayloadDeserializers.from(
            new ProposalPayload(ROUND_IDENTIFIER, Hash.fromHexStringLenient("0x1234")), sig);
    final List<SignedData<PreparePayload>> prepares = new ArrayList<>();
    for (int i = 0; i < prepareCount; i++) {
      prepares.add(
          PayloadDeserializers.from(
              new PreparePayload(ROUND_IDENTIFIER, Hash.fromHexStringLenient("0x1234")), sig));
    }
    final PreparedCertificate preparedCert = new PreparedCertificate(signedProposal, prepares);
    final RoundChangePayload payload =
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.of(preparedCert));
    return PayloadDeserializers.from(payload, sig);
  }

  @Test
  public void decodeRejectsNestedSignedPayloadsExceedingBudget() {
    final RoundChangeCertificate certificate =
        new RoundChangeCertificate(
            Collections.nCopies(3, fakeSignedRoundChangeWithPreparedCert(2)));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    certificate.writeTo(out);

    assertThatThrownBy(
            () -> RoundChangeCertificate.readFrom(RLP.input(out.encoded()), new DecodeBudget(11)))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceed the maximum permitted total");
  }

  @Test
  public void decodeAcceptsNestedSignedPayloadsAtBudget() {
    final RoundChangeCertificate certificate =
        new RoundChangeCertificate(
            Collections.nCopies(3, fakeSignedRoundChangeWithPreparedCert(2)));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    certificate.writeTo(out);

    final RoundChangeCertificate decoded =
        RoundChangeCertificate.readFrom(RLP.input(out.encoded()), new DecodeBudget(12));
    assertThat(decoded.getRoundChangePayloads()).hasSize(3);
  }

  @Test
  public void decodeRejectsRoundChangePayloadsExceedingMaxEntries() {
    final RoundChangeCertificate oversized =
        new RoundChangeCertificate(
            Collections.nCopies(BftMessage.MAX_LIST_ENTRIES + 1, fakeSignedRoundChange()));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    oversized.writeTo(out);

    assertThatThrownBy(
            () ->
                RoundChangeCertificate.readFrom(
                    RLP.input(out.encoded()), DecodeBudget.forSingleMessage()))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceeds the maximum permitted size");
  }

  @Test
  public void decodeAcceptsRoundChangePayloadsAtMaxEntries() {
    final RoundChangeCertificate atLimit =
        new RoundChangeCertificate(
            Collections.nCopies(BftMessage.MAX_LIST_ENTRIES, fakeSignedRoundChange()));
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    atLimit.writeTo(out);

    final RoundChangeCertificate decoded =
        RoundChangeCertificate.readFrom(RLP.input(out.encoded()), DecodeBudget.forSingleMessage());
    assertThat(decoded.getRoundChangePayloads()).hasSize(BftMessage.MAX_LIST_ENTRIES);
  }

  @Test
  public void rlpRoundTripWithNoPreparedCertificate() {
    final RoundChangePayload roundChangePayload =
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.empty());
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    roundChangePayload.writeTo(rlpOut);

    final RLPInput rlpInput = RLP.input(rlpOut.encoded());
    RoundChangePayload actualRoundChangePayload =
        RoundChangePayload.readFrom(rlpInput, DecodeBudget.forSingleMessage());

    assertThat(actualRoundChangePayload.getPreparedCertificate()).isEqualTo(Optional.empty());
    assertThat(actualRoundChangePayload.getRoundIdentifier()).isEqualTo(ROUND_IDENTIFIER);
    assertThat(actualRoundChangePayload.getMessageType()).isEqualTo(IbftV2.ROUND_CHANGE);
  }

  @Test
  public void rlpRoundTripWithPreparedCertificate() {
    final Block block =
        ProposedBlockHelpers.createProposalBlock(
            singletonList(AddressHelpers.ofValue(1)), ROUND_IDENTIFIER);
    final ProposalPayload proposalPayload = new ProposalPayload(ROUND_IDENTIFIER, block.getHash());
    final SECPSignature signature =
        SignatureAlgorithmFactory.getInstance()
            .createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);
    SignedData<ProposalPayload> signedProposal =
        PayloadDeserializers.from(proposalPayload, signature);

    final PreparePayload preparePayload =
        new PreparePayload(ROUND_IDENTIFIER, Hash.fromHexStringLenient("0x8523ba6e7c5f59ae87"));
    final SignedData<PreparePayload> signedPrepare =
        PayloadDeserializers.from(preparePayload, signature);

    final PreparedCertificate preparedCert =
        new PreparedCertificate(signedProposal, Lists.newArrayList(signedPrepare));

    final RoundChangePayload roundChangePayload =
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.of(preparedCert));
    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    roundChangePayload.writeTo(rlpOut);

    final RLPInput rlpInput = RLP.input(rlpOut.encoded());
    RoundChangePayload actualRoundChangePayload =
        RoundChangePayload.readFrom(rlpInput, DecodeBudget.forSingleMessage());

    assertThat(actualRoundChangePayload.getPreparedCertificate())
        .isEqualTo(Optional.of(preparedCert));
    assertThat(actualRoundChangePayload.getRoundIdentifier()).isEqualTo(ROUND_IDENTIFIER);
    assertThat(actualRoundChangePayload.getMessageType()).isEqualTo(IbftV2.ROUND_CHANGE);
  }
}
