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
package org.hyperledger.besu.consensus.ibft.tests.round;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithBftExtraDataEncoder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftBlockCreator;
import org.hyperledger.besu.consensus.common.bft.inttest.StubValidatorMulticaster;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.network.IbftMessageTransmitter;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.statemachine.IbftRound;
import org.hyperledger.besu.consensus.ibft.statemachine.RoundState;
import org.hyperledger.besu.consensus.ibft.validation.MessageValidator;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;
import org.hyperledger.besu.util.Subscribers;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IbftRoundIntegrationTest {

  private final MessageFactory peerMessageFactory = new MessageFactory(NodeKeyUtils.generate());
  private final MessageFactory peerMessageFactory2 = new MessageFactory(NodeKeyUtils.generate());
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 0);
  private final Subscribers<MinedBlockObserver> subscribers = Subscribers.create();
  private ProtocolContext protocolContext;

  @Mock private BftProtocolSchedule protocolSchedule;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private MutableBlockchain blockChain;
  @Mock private WorldStateArchive worldStateArchive;
  @Mock private BlockImporter blockImporter;

  @Mock private BftBlockCreator blockCreator;
  @Mock private MessageValidator messageValidator;
  @Mock private RoundTimer roundTimer;
  @Mock private NodeKey nodeKey;
  private MessageFactory throwingMessageFactory;
  private IbftMessageTransmitter transmitter;
  @Mock private StubValidatorMulticaster multicaster;
  @Mock BlockHeader parentHeader;

  private Block proposedBlock;

  private final SECPSignature remoteCommitSeal =
      SignatureAlgorithmFactory.getInstance()
          .createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 1);
  private final IbftExtraDataCodec bftExtraDataEncoder = new IbftExtraDataCodec();

  @BeforeEach
  public void setup() {
    when(messageValidator.validateProposal(any())).thenReturn(true);
    when(messageValidator.validatePrepare(any())).thenReturn(true);
    when(messageValidator.validateCommit(any())).thenReturn(true);

    when(nodeKey.sign(any())).thenThrow(new SecurityModuleException("Hsm Is Down"));

    throwingMessageFactory = new MessageFactory(nodeKey);
    transmitter = new IbftMessageTransmitter(throwingMessageFactory, multicaster);

    final BftExtraData proposedExtraData =
        new BftExtraData(Bytes.wrap(new byte[32]), emptyList(), empty(), 0, emptyList());
    final BlockHeaderTestFixture headerTestFixture = new BlockHeaderTestFixture();
    headerTestFixture.extraData(bftExtraDataEncoder.encode(proposedExtraData));
    headerTestFixture.number(1);
    final BlockHeader header = headerTestFixture.buildHeader();
    proposedBlock = new Block(header, new BlockBody(emptyList(), emptyList()));

    when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);
    when(protocolSpec.getBlockImporter()).thenReturn(blockImporter);

    when(blockImporter.importBlock(any(), any(), any(), any(), any()))
        .thenReturn(new BlockImportResult(true));

    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockChain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(emptyList(), bftExtraDataEncoder))
            .build();
  }

  @Test
  public void signingFailsOnReceiptOfProposalUpdatesRoundButTransmitsNothing() {
    final int QUORUM_SIZE = 1;
    final RoundState roundState = new RoundState(roundIdentifier, QUORUM_SIZE, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            throwingMessageFactory,
            transmitter,
            roundTimer,
            bftExtraDataEncoder,
            parentHeader);

    round.handleProposalMessage(
        peerMessageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));

    assertThat(roundState.getProposedBlock()).isNotEmpty();
    assertThat(roundState.isPrepared()).isTrue();
    assertThat(roundState.isCommitted()).isFalse();

    verifyNoInteractions(multicaster);
  }

  @Test
  public void failuresToSignStillAllowBlockToBeImported() {
    final int QUORUM_SIZE = 2;
    final RoundState roundState = new RoundState(roundIdentifier, QUORUM_SIZE, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            throwingMessageFactory,
            transmitter,
            roundTimer,
            bftExtraDataEncoder,
            parentHeader);

    // inject a block first, then a prepare on it.
    round.handleProposalMessage(
        peerMessageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));

    assertThat(roundState.getProposedBlock()).isNotEmpty();
    assertThat(roundState.isPrepared()).isFalse();
    assertThat(roundState.isCommitted()).isFalse();

    round.handlePrepareMessage(peerMessageFactory.createPrepare(roundIdentifier, Hash.EMPTY));

    assertThat(roundState.getProposedBlock()).isNotEmpty();
    assertThat(roundState.isPrepared()).isTrue();
    assertThat(roundState.isCommitted()).isFalse();
    verifyNoInteractions(multicaster);

    round.handleCommitMessage(
        peerMessageFactory.createCommit(roundIdentifier, Hash.EMPTY, remoteCommitSeal));
    assertThat(roundState.isCommitted()).isFalse();
    verifyNoInteractions(multicaster);

    round.handleCommitMessage(
        peerMessageFactory2.createCommit(roundIdentifier, Hash.EMPTY, remoteCommitSeal));
    assertThat(roundState.isCommitted()).isTrue();
    verifyNoInteractions(multicaster);

    verify(blockImporter).importBlock(any(), any(), any(), any(), any());
  }
}
