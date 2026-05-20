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
package org.hyperledger.besu.consensus.ibft.statemachine;

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.consensus.common.bft.BftContextBuilder.setupContextWithBftExtraDataEncoder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.bft.BftBlockHashing;
import org.hyperledger.besu.consensus.common.bft.BftExtraData;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftProtocolSchedule;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftBlockCreator;
import org.hyperledger.besu.consensus.ibft.IbftExtraDataCodec;
import org.hyperledger.besu.consensus.ibft.network.IbftMessageTransmitter;
import org.hyperledger.besu.consensus.ibft.payload.MessageFactory;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.consensus.ibft.validation.MessageValidator;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.cryptoservices.NodeKeyUtils;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreationTiming;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator.BlockCreationResult;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionSelectionResults;
import org.hyperledger.besu.ethereum.chain.MinedBlockObserver;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.mainnet.BlockImportResult;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.WithdrawalsValidator;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;
import org.hyperledger.besu.util.Subscribers;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IbftRoundTest {

  private final NodeKey nodeKey = NodeKeyUtils.generate();
  private final NodeKey nodeKey2 = NodeKeyUtils.generate();
  private final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 0);
  private final MessageFactory messageFactory = new MessageFactory(nodeKey);
  private final MessageFactory messageFactory2 = new MessageFactory(nodeKey2);
  private final Subscribers<MinedBlockObserver> subscribers = Subscribers.create();
  private final BftExtraDataCodec bftExtraDataCodec = new IbftExtraDataCodec();
  private ProtocolContext protocolContext;

  @Mock private BftProtocolSchedule protocolSchedule;
  @Mock private MutableBlockchain blockChain;
  @Mock private WorldStateArchive worldStateArchive;
  @Mock private IbftMessageTransmitter transmitter;
  @Mock private MinedBlockObserver minedBlockObserver;
  @Mock private BftBlockCreator blockCreator;
  @Mock private MessageValidator messageValidator;
  @Mock private RoundTimer roundTimer;
  @Mock private ProtocolSpec protocolSpec;
  @Mock private BlockImporter blockImporter;
  @Mock private BlockHeader parentHeader;

  @Captor private ArgumentCaptor<Block> blockCaptor;

  private Block proposedBlock;
  private BftExtraData proposedExtraData;

  private final SECPSignature remoteCommitSeal =
      SignatureAlgorithmFactory.getInstance()
          .createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 1);

  @BeforeEach
  public void setup() {
    protocolContext =
        new ProtocolContext.Builder()
            .withBlockchain(blockChain)
            .withWorldStateArchive(worldStateArchive)
            .withConsensusContext(
                setupContextWithBftExtraDataEncoder(emptyList(), new IbftExtraDataCodec()))
            .build();

    lenient().when(messageValidator.validateProposal(any())).thenReturn(true);
    lenient().when(messageValidator.validatePrepare(any())).thenReturn(true);
    lenient().when(messageValidator.validateCommit(any())).thenReturn(true);

    proposedExtraData =
        new BftExtraData(Bytes.wrap(new byte[32]), emptyList(), empty(), 0, emptyList());
    final BlockHeaderTestFixture headerTestFixture = new BlockHeaderTestFixture();
    headerTestFixture.extraData(new IbftExtraDataCodec().encode(proposedExtraData));
    headerTestFixture.number(1);

    final BlockHeader header = headerTestFixture.buildHeader();
    proposedBlock = new Block(header, new BlockBody(emptyList(), emptyList()));

    lenient()
        .when(blockCreator.createBlock(anyLong(), any()))
        .thenReturn(
            new BlockCreationResult(
                proposedBlock,
                new TransactionSelectionResults(),
                new BlockCreationTiming(),
                Optional.empty()));

    lenient().when(protocolSpec.getBlockImporter()).thenReturn(blockImporter);
    lenient().when(protocolSchedule.getByBlockHeader(any())).thenReturn(protocolSpec);

    lenient()
        .when(blockImporter.importBlock(any(), any(), any(), any(), any()))
        .thenReturn(new BlockImportResult(BlockImportResult.BlockImportStatus.IMPORTED));

    subscribers.subscribe(minedBlockObserver);
  }

  @Test
  public void onConstructionRoundTimerIsStarted() {
    final RoundState roundState = new RoundState(roundIdentifier, 3, messageValidator);
    new IbftRound(
        roundState,
        blockCreator,
        protocolContext,
        protocolSchedule,
        subscribers,
        nodeKey,
        messageFactory,
        transmitter,
        roundTimer,
        bftExtraDataCodec,
        parentHeader);
    verify(roundTimer, times(1)).startTimer(roundIdentifier);
  }

  @Test
  public void onReceptionOfValidProposalSendsAPrepareToNetworkPeers() {
    final RoundState roundState = new RoundState(roundIdentifier, 3, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    round.handleProposalMessage(
        messageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));
    verify(transmitter, times(1)).multicastPrepare(roundIdentifier, proposedBlock.getHash());
    verify(transmitter, never()).multicastCommit(any(), any(), any());
  }

  @Test
  public void sendsAProposalWhenRequested() {
    final RoundState roundState = new RoundState(roundIdentifier, 3, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    round.createAndSendProposalMessage(15);
    verify(transmitter, times(1))
        .multicastProposal(roundIdentifier, proposedBlock, Optional.empty(), Optional.empty());
    verify(transmitter, never()).multicastPrepare(any(), any());
    verify(transmitter, never()).multicastCommit(any(), any(), any());
  }

  @Test
  public void singleValidatorImportBlocksImmediatelyOnProposalCreation() {
    final RoundState roundState = new RoundState(roundIdentifier, 1, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);
    round.createAndSendProposalMessage(15);
    verify(transmitter, times(1))
        .multicastProposal(roundIdentifier, proposedBlock, Optional.empty(), Optional.empty());
    verify(transmitter, never()).multicastPrepare(any(), any());
    verify(transmitter, times(1)).multicastCommit(any(), any(), any());
    verify(blockImporter, times(1)).importBlock(any(), any(), any(), any(), any());
  }

  @Test
  public void twoValidatorNetworkSendsPrepareOnProposalReceptionThenSendsCommitOnCommitReceive() {
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    final Hash commitSealHash =
        new BftBlockHashing(new IbftExtraDataCodec())
            .calculateDataHashForCommittedSeal(proposedBlock.getHeader(), proposedExtraData);
    final SECPSignature localCommitSeal = nodeKey.sign(Bytes32.wrap(commitSealHash.getBytes()));

    // Receive Proposal Message
    round.handleProposalMessage(
        messageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));
    verify(transmitter, times(1)).multicastPrepare(roundIdentifier, proposedBlock.getHash());
    verify(transmitter, times(1))
        .multicastCommit(roundIdentifier, proposedBlock.getHash(), localCommitSeal);
    verify(blockImporter, never()).importBlock(any(), any(), any(), any(), any());

    // Receive Commit Message

    round.handleCommitMessage(
        messageFactory2.createCommit(roundIdentifier, proposedBlock.getHash(), remoteCommitSeal));

    // Should import block when both commit seals are available.
    final ArgumentCaptor<Block> capturedBlock = ArgumentCaptor.forClass(Block.class);
    verify(blockImporter, times(1))
        .importBlock(any(), capturedBlock.capture(), any(), any(), any());

    // Ensure imported block contains both commit seals.
    final BftExtraData importedExtraData =
        new IbftExtraDataCodec().decode(capturedBlock.getValue().getHeader());
    assertThat(importedExtraData.getSeals()).containsOnly(remoteCommitSeal, localCommitSeal);
  }

  @Test
  public void localNodeProposesToNetworkOfTwoValidatorsImportsOnReceptionOfCommitFromPeer() {
    lenient()
        .when(protocolSpec.getWithdrawalsValidator())
        .thenReturn(new WithdrawalsValidator.AllowedWithdrawals());
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    final Hash commitSealHash =
        new BftBlockHashing(new IbftExtraDataCodec())
            .calculateDataHashForCommittedSeal(proposedBlock.getHeader(), proposedExtraData);
    final SECPSignature localCommitSeal = nodeKey.sign(Bytes32.wrap(commitSealHash.getBytes()));

    round.createAndSendProposalMessage(15);
    verify(transmitter, never()).multicastCommit(any(), any(), any());
    verify(blockImporter, never()).importBlock(any(), any(), any(), any(), any());

    round.handlePrepareMessage(
        messageFactory2.createPrepare(roundIdentifier, proposedBlock.getHash()));

    verify(transmitter, times(1))
        .multicastCommit(roundIdentifier, proposedBlock.getHash(), localCommitSeal);
    verify(blockImporter, never()).importBlock(any(), any(), any(), any(), any());

    round.handleCommitMessage(
        messageFactory2.createCommit(roundIdentifier, proposedBlock.getHash(), remoteCommitSeal));
    verify(blockImporter, times(1)).importBlock(any(), any(), any(), any(), any());
  }

  @Test
  public void aProposalWithAnewBlockIsSentUponReceptionOfARoundChangeWithNoCertificate() {
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    final RoundChangeCertificate roundChangeCertificate = new RoundChangeCertificate(emptyList());

    round.startRoundWith(new RoundChangeArtifacts(empty(), empty(), emptyList()), 15);
    verify(transmitter, times(1))
        .multicastProposal(
            eq(roundIdentifier), any(), any(), eq(Optional.of(roundChangeCertificate)));
  }

  @Test
  public void aProposalMessageWithTheSameBlockIsSentUponReceptionOfARoundChangeWithCertificate() {
    final ConsensusRoundIdentifier priorRoundChange = new ConsensusRoundIdentifier(1, 0);
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    final RoundChangeArtifacts roundChangeArtifacts =
        RoundChangeArtifacts.create(
            Collections.singletonList(
                messageFactory.createRoundChange(
                    roundIdentifier,
                    Optional.of(
                        new PreparedRoundArtifacts(
                            messageFactory.createProposal(
                                priorRoundChange, proposedBlock, Optional.empty()),
                            emptyList())))));

    // NOTE: IbftRound assumes the prepare's are valid

    round.startRoundWith(roundChangeArtifacts, 15);
    verify(transmitter, times(1))
        .multicastProposal(
            eq(roundIdentifier),
            blockCaptor.capture(),
            any(),
            eq(Optional.of(roundChangeArtifacts.getRoundChangeCertificate())));

    final BftExtraData proposedExtraData =
        new IbftExtraDataCodec().decode(blockCaptor.getValue().getHeader());
    assertThat(proposedExtraData.getRound()).isEqualTo(roundIdentifier.getRoundNumber());

    // Inject a single Prepare message, and confirm the roundState has gone to Prepared (which
    // indicates the block has entered the roundState (note: all msgs are deemed valid due to mocks)
    round.handlePrepareMessage(
        messageFactory.createPrepare(roundIdentifier, proposedBlock.getHash()));
    assertThat(roundState.isPrepared()).isTrue();
  }

  @Test
  public void creatingNewBlockFromEmptyPreparedCertificateUpdatesInternalState() {
    final RoundState roundState = new RoundState(roundIdentifier, 2, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    final RoundChangeArtifacts roundChangeArtifacts =
        RoundChangeArtifacts.create(
            Collections.singletonList(messageFactory.createRoundChange(roundIdentifier, empty())));

    round.startRoundWith(roundChangeArtifacts, 15);
    verify(transmitter, times(1))
        .multicastProposal(
            eq(roundIdentifier),
            blockCaptor.capture(),
            any(),
            eq(Optional.of(roundChangeArtifacts.getRoundChangeCertificate())));

    // Inject a single Prepare message, and confirm the roundState has gone to Prepared (which
    // indicates the block has entered the roundState (note: all msgs are deemed valid due to mocks)
    round.handlePrepareMessage(
        messageFactory.createPrepare(roundIdentifier, proposedBlock.getHash()));
    assertThat(roundState.isPrepared()).isTrue();
  }

  @Test
  public void creatingNewBlockNotifiesBlockMiningObservers() {
    final RoundState roundState = new RoundState(roundIdentifier, 1, messageValidator);
    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            nodeKey,
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);
    round.createAndSendProposalMessage(15);
    verify(minedBlockObserver).blockMined(any());
  }

  @Test
  public void blockIsOnlyImportedOnceWhenCommitsAreReceivedBeforeProposal() {
    final ConsensusRoundIdentifier roundIdentifier = new ConsensusRoundIdentifier(1, 0);
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
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    round.handleCommitMessage(
        messageFactory.createCommit(roundIdentifier, proposedBlock.getHash(), remoteCommitSeal));

    round.handleCommitMessage(
        messageFactory2.createCommit(roundIdentifier, proposedBlock.getHash(), remoteCommitSeal));

    round.handleProposalMessage(
        messageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));

    verify(blockImporter, times(1)).importBlock(any(), any(), any(), any(), any());
  }

  @Test
  public void blockIsImportedOnlyOnceIfQuorumCommitsAreReceivedPriorToProposal() {
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
            messageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    round.handleCommitMessage(
        messageFactory.createCommit(roundIdentifier, proposedBlock.getHash(), remoteCommitSeal));

    round.handleProposalMessage(
        messageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));

    verify(blockImporter, times(1)).importBlock(any(), any(), any(), any(), any());
  }

  @Test
  public void exceptionDuringNodeKeySigningDoesNotEscape() {
    final int QUORUM_SIZE = 1;
    final RoundState roundState = new RoundState(roundIdentifier, QUORUM_SIZE, messageValidator);
    final NodeKey throwingNodeKey = mock(NodeKey.class);
    final MessageFactory throwingMessageFactory = new MessageFactory(throwingNodeKey);
    when(throwingNodeKey.sign(any())).thenThrow(new SecurityModuleException("Hsm is Offline"));

    final IbftRound round =
        new IbftRound(
            roundState,
            blockCreator,
            protocolContext,
            protocolSchedule,
            subscribers,
            throwingNodeKey,
            throwingMessageFactory,
            transmitter,
            roundTimer,
            bftExtraDataCodec,
            parentHeader);

    round.handleProposalMessage(
        messageFactory.createProposal(roundIdentifier, proposedBlock, Optional.empty()));

    // Verify that no prepare message was constructed by the IbftRound
    assertThat(
            roundState
                .constructPreparedRoundArtifacts()
                .get()
                .getPreparedCertificate()
                .getPreparePayloads())
        .isEmpty();

    verifyNoInteractions(transmitter);
  }
}
