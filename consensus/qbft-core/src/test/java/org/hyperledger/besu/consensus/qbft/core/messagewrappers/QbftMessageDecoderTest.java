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
package org.hyperledger.besu.consensus.qbft.core.messagewrappers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.CommitMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.PrepareMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.ProposalMessageData;
import org.hyperledger.besu.consensus.qbft.core.messagedata.RoundChangeMessageData;
import org.hyperledger.besu.consensus.qbft.core.payload.CommitPayload;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.ProposalPayload;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCodec;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class QbftMessageDecoderTest {

  @Mock private QbftBlockCodec blockEncoder;
  @Mock private QbftBlock block;

  private final QbftMessageDecoder decoder = new QbftMessageDecoder();

  @Test
  public void decodeSequenceForPrepare() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final PreparePayload payload =
        new PreparePayload(new ConsensusRoundIdentifier(10L, 0), Hash.ZERO);
    final SignedData<PreparePayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    final MessageData messageData = PrepareMessageData.create(new Prepare(signed));

    assertThat(decoder.decodeSequence(messageData)).isEqualTo(10L);
  }

  @Test
  public void decodeSequenceForCommit() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final CommitPayload payload =
        new CommitPayload(
            new ConsensusRoundIdentifier(20L, 0),
            Hash.ZERO,
            SignatureAlgorithmFactory.getInstance()
                .createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0));
    final SignedData<CommitPayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    final MessageData messageData = CommitMessageData.create(new Commit(signed));

    assertThat(decoder.decodeSequence(messageData)).isEqualTo(20L);
  }

  @Test
  public void decodeSequenceForProposal() {
    doAnswer(
            inv -> {
              inv.getArgument(1, RLPOutput.class).writeNull();
              return null;
            })
        .when(blockEncoder)
        .writeTo(any(QbftBlock.class), any(RLPOutput.class));

    final NodeKey nodeKey = NodeKeyUtils.generate();
    final ProposalPayload payload =
        new ProposalPayload(new ConsensusRoundIdentifier(30L, 0), block, blockEncoder);
    final SignedData<ProposalPayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    final MessageData messageData =
        ProposalMessageData.create(new Proposal(signed, List.of(), List.of()));

    assertThat(decoder.decodeSequence(messageData)).isEqualTo(30L);
  }

  @Test
  public void decodeSequenceForRoundChange() {
    final NodeKey nodeKey = NodeKeyUtils.generate();
    final RoundChangePayload payload =
        new RoundChangePayload(new ConsensusRoundIdentifier(40L, 0), Optional.empty());
    final SignedData<RoundChangePayload> signed =
        SignedData.create(
            payload, nodeKey.sign(Bytes32.wrap(payload.hashForSignature().getBytes())));
    final MessageData messageData =
        RoundChangeMessageData.create(
            new RoundChange(signed, Optional.empty(), Optional.empty(), blockEncoder, List.of()));

    assertThat(decoder.decodeSequence(messageData)).isEqualTo(40L);
  }

  @Test
  public void decodeSequenceThrowsForUnknownCode() {
    final MessageData unknown = mock(MessageData.class);
    when(unknown.getCode()).thenReturn(0xFF);

    assertThatThrownBy(() -> decoder.decodeSequence(unknown))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void decodeSequenceThrowsRlpExceptionForMalformedPrepare() {
    final MessageData malformed = mock(MessageData.class);
    when(malformed.getCode())
        .thenReturn(org.hyperledger.besu.consensus.qbft.core.messagedata.QbftV1.PREPARE);
    when(malformed.getData()).thenReturn(Bytes.of(0x01, 0x02, 0x03));

    assertThatThrownBy(() -> decoder.decodeSequence(malformed)).isInstanceOf(RLPException.class);
  }
}
