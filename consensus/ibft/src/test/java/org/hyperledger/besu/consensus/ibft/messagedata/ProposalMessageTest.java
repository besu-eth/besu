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
package org.hyperledger.besu.consensus.ibft.messagedata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.ibft.TestHelpers;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProposalMessageTest {
  @Mock private Proposal proposalMessageData;
  @Mock private Bytes messageBytes;
  @Mock private MessageData messageData;
  @Mock private ProposalMessageData proposalMessage;

  @Test
  public void createMessageFromPrePrepareMessageData() {
    when(proposalMessageData.encode()).thenReturn(messageBytes);
    final ProposalMessageData proposalMessage = ProposalMessageData.create(proposalMessageData);

    assertThat(proposalMessage.getData()).isEqualTo(messageBytes);
    assertThat(proposalMessage.getCode()).isEqualTo(IbftV2.PROPOSAL);
    verify(proposalMessageData).encode();
  }

  @Test
  public void createMessageFromPrePrepareMessage() {
    final ProposalMessageData message = ProposalMessageData.fromMessageData(proposalMessage);
    assertThat(message).isSameAs(proposalMessage);
  }

  @Test
  public void createMessageFromGenericMessageData() {
    when(messageData.getCode()).thenReturn(IbftV2.PROPOSAL);
    when(messageData.getData()).thenReturn(messageBytes);
    final ProposalMessageData proposalMessage = ProposalMessageData.fromMessageData(messageData);

    assertThat(proposalMessage.getData()).isEqualTo(messageBytes);
    assertThat(proposalMessage.getCode()).isEqualTo(IbftV2.PROPOSAL);
  }

  @Test
  public void createMessageWithBlockAccessList() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal expectedProposal =
        TestHelpers.createSignedProposalPayload(
            nodeKey, Optional.of(BlockAccessList.builder().build()));
    final ProposalMessageData expectedMessage = ProposalMessageData.create(expectedProposal);
    when(messageData.getCode()).thenReturn(expectedMessage.getCode());
    when(messageData.getData()).thenReturn(expectedMessage.getData());
    final ProposalMessageData proposalMessage = ProposalMessageData.fromMessageData(messageData);

    assertThat(proposalMessage.getData()).isEqualTo(expectedMessage.getData());
    assertThat(proposalMessage.getCode()).isEqualTo(expectedMessage.getCode());
    final Proposal decodedProposal = proposalMessage.decode();
    assertThat(decodedProposal).isEqualTo(expectedProposal);
    assertThat(decodedProposal.getBlockAccessList()).isPresent();
  }

  @Test
  public void createMessageWithoutBlockAccessList() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal expectedProposal = TestHelpers.createSignedProposalPayload(nodeKey);
    final ProposalMessageData expectedMessage = ProposalMessageData.create(expectedProposal);
    when(messageData.getCode()).thenReturn(expectedMessage.getCode());
    when(messageData.getData()).thenReturn(expectedMessage.getData());
    final ProposalMessageData proposalMessage = ProposalMessageData.fromMessageData(messageData);

    assertThat(proposalMessage.getData()).isEqualTo(expectedMessage.getData());
    assertThat(proposalMessage.getCode()).isEqualTo(expectedMessage.getCode());
    final Proposal decodedProposal = proposalMessage.decode();
    assertThat(decodedProposal).isEqualTo(expectedProposal);
    assertThat(decodedProposal.getBlockAccessList()).isEmpty();
  }

  @Test
  public void createMessageFailsWhenIncorrectMessageCode() {
    when(messageData.getCode()).thenReturn(42);
    assertThatThrownBy(() -> ProposalMessageData.fromMessageData(messageData))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("MessageData has code 42 and thus is not a ProposalMessageData");
  }

  @Test
  public void defaultEncodingEmitsCurrentFourItemWireFormat() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal proposal = TestHelpers.createSignedProposalPayload(nodeKey);

    final RLPInput rlpIn = RLP.input(proposal.encode());
    assertThat(rlpIn.enterList()).isEqualTo(4);
  }

  @Test
  public void legacyEncodingOmitsBlockAccessListSlot() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal reference = TestHelpers.createSignedProposalPayload(nodeKey);

    final Proposal legacy =
        Proposal.withLegacyEncoding(
            reference.getSignedPayload(),
            reference.getBlock(),
            reference.getBlockAccessList(),
            reference.getRoundChangeCertificate());

    final RLPInput rlpIn = RLP.input(legacy.encode());
    assertThat(rlpIn.enterList()).isEqualTo(3);
  }

  @Test
  public void legacyEncodingSkipsBalEvenWhenPresent() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal reference = TestHelpers.createSignedProposalPayload(nodeKey);

    final Proposal legacy =
        Proposal.withLegacyEncoding(
            reference.getSignedPayload(),
            reference.getBlock(),
            Optional.of(BlockAccessList.builder().build()),
            reference.getRoundChangeCertificate());

    final RLPInput rlpIn = RLP.input(legacy.encode());
    assertThat(rlpIn.enterList()).isEqualTo(3);
  }

  @Test
  public void canDecodeProposalFromLegacyNodeWithoutBlockAccessList() {
    // Pre-26.1.0 wire format: [SignedPayload, Block, RoundChangeCertificate-or-null]            (3
    // items)
    // Current wire format:    [SignedPayload, Block, RoundChangeCertificate-or-null, BAL-or-null]
    // (4 items)
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final Proposal referenceProposal = TestHelpers.createSignedProposalPayload(nodeKey);

    final BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
    rlpOut.startList();
    referenceProposal.getSignedPayload().writeTo(rlpOut);
    referenceProposal.getBlock().writeTo(rlpOut);
    referenceProposal
        .getRoundChangeCertificate()
        .ifPresentOrElse(rcc -> rcc.writeTo(rlpOut), rlpOut::writeNull);
    rlpOut.endList();
    final Bytes legacyEncoded = rlpOut.encoded();

    when(messageData.getCode()).thenReturn(IbftV2.PROPOSAL);
    when(messageData.getData()).thenReturn(legacyEncoded);
    final ProposalMessageData legacyMessage = ProposalMessageData.fromMessageData(messageData);

    final Proposal decoded = legacyMessage.decode();

    assertThat(decoded.getSignedPayload()).isEqualTo(referenceProposal.getSignedPayload());
    assertThat(decoded.getBlockAccessList()).isEmpty();
  }
}
