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
package org.hyperledger.besu.consensus.ibft.messagewrappers;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.ProposedBlockHelpers;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.payload.PreparePayload;
import org.hyperledger.besu.consensus.ibft.payload.PreparedCertificate;
import org.hyperledger.besu.consensus.ibft.payload.ProposalPayload;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangePayload;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage of {@link Proposal#decode(Bytes, int)}: the actual entry point {@code
 * IbftController} uses, which derives a {@link
 * org.hyperledger.besu.consensus.ibft.payload.DecodeBudget} from the current validator count and
 * threads it through the nested {@code RoundChangeCertificate}/{@code PreparedCertificate} decode.
 * {@code DecodeBudgetTest}, {@code RoundChangeCertificateTest} and {@code PreparedCertificateTest}
 * cover the individual pieces; this verifies the wiring between them.
 */
public class ProposalTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final SECPSignature SIG =
      SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.TEN, (byte) 0);
  private static final ConsensusRoundIdentifier ROUND_IDENTIFIER =
      new ConsensusRoundIdentifier(0x1234567890ABCDEFL, 0xFEDCBA98);

  private SignedData<RoundChangePayload> fakeSignedRoundChange() {
    return SignedData.create(new RoundChangePayload(ROUND_IDENTIFIER, Optional.empty()), SIG);
  }

  private SignedData<RoundChangePayload> fakeSignedRoundChangeWithPreparedCert(
      final int prepareCount) {
    final SignedData<ProposalPayload> signedProposal =
        SignedData.create(new ProposalPayload(ROUND_IDENTIFIER, Hash.ZERO), SIG);
    final List<SignedData<PreparePayload>> prepares = new ArrayList<>();
    for (int i = 0; i < prepareCount; i++) {
      prepares.add(SignedData.create(new PreparePayload(ROUND_IDENTIFIER, Hash.ZERO), SIG));
    }
    final PreparedCertificate preparedCert = new PreparedCertificate(signedProposal, prepares);
    return SignedData.create(
        new RoundChangePayload(ROUND_IDENTIFIER, Optional.of(preparedCert)), SIG);
  }

  private Proposal proposalWithRoundChanges(
      final List<SignedData<RoundChangePayload>> roundChanges) {
    final SignedData<ProposalPayload> topPayload =
        SignedData.create(new ProposalPayload(ROUND_IDENTIFIER, Hash.ZERO), SIG);
    final Block block =
        ProposedBlockHelpers.createProposalBlock(
            singletonList(AddressHelpers.ofValue(1)), ROUND_IDENTIFIER);
    return new Proposal(
        topPayload, block, Optional.empty(), Optional.of(new RoundChangeCertificate(roundChanges)));
  }

  @Test
  public void decodeAcceptsNestedSignedPayloadsAtLimit() {
    // validatorCount=3 => budget (3+1)^2=16. 3 round changes, each with a 3-prepare certificate:
    // 1 (top proposal) + 3 * (1 round-change + 1 nested-proposal + 3 prepares) = 1 + 3*5 = 16.
    final int validatorCount = 3;
    final List<SignedData<RoundChangePayload>> roundChanges =
        Collections.nCopies(3, fakeSignedRoundChangeWithPreparedCert(3));
    final Bytes encoded = proposalWithRoundChanges(roundChanges).encode();

    final Proposal decoded = Proposal.decode(encoded, validatorCount);

    assertThat(decoded.getRoundChangeCertificate()).isPresent();
    assertThat(decoded.getRoundChangeCertificate().get().getRoundChangePayloads()).hasSize(3);
  }

  @Test
  public void decodeRejectsNestedSignedPayloadsExceedingLimit() {
    // validatorCount=3 => limit (3+1)^2=16. 3 round changes, each with a 3-prepare certificate,
    // plus one bare round change: 1 (top proposal) + 3 * (1 round-change + 1 nested-proposal + 3
    // prepares) + 1 (extra round-change) = 1 + 3*5 + 1 = 17, one over the limit of 16.
    final int validatorCount = 3;
    final List<SignedData<RoundChangePayload>> roundChanges =
        new ArrayList<>(Collections.nCopies(3, fakeSignedRoundChangeWithPreparedCert(3)));
    roundChanges.add(fakeSignedRoundChange());
    final Bytes encoded = proposalWithRoundChanges(roundChanges).encode();

    assertThatThrownBy(() -> Proposal.decode(encoded, validatorCount))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("exceed the maximum permitted total");
  }

  @Test
  public void decodeWithoutValidatorCountIgnoresLimit() {
    final List<SignedData<RoundChangePayload>> roundChanges =
        new ArrayList<>(Collections.nCopies(3, fakeSignedRoundChangeWithPreparedCert(3)));
    roundChanges.add(fakeSignedRoundChange());
    final Bytes encoded = proposalWithRoundChanges(roundChanges).encode();

    final Proposal decoded = Proposal.decode(encoded);

    assertThat(decoded.getRoundChangeCertificate().get().getRoundChangePayloads()).hasSize(4);
  }
}
