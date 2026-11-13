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
package org.hyperledger.besu.consensus.qbft.core.statemachine;

import static java.util.Collections.emptyList;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.RoundTimer;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Commit;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.network.QbftMessageTransmitter;
import org.hyperledger.besu.consensus.qbft.core.payload.MessageFactory;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreator.BlockCreationResult;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockImporter;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.types.QbftMinedBlockObserver;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.securitymodule.SecurityModuleException;
import org.hyperledger.besu.util.Subscribers;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Qbft round. */
public class QbftRound {

  private static final Logger LOG = LoggerFactory.getLogger(QbftRound.class);

  private final Subscribers<QbftMinedBlockObserver> observers;

  /** The Round state. */
  protected final RoundState roundState;

  /** The Block creator. */
  protected final QbftBlockCreator blockCreator;

  /** The Protocol context. */
  protected final QbftBlockInterface blockInterface;

  /** The Protocol schedule. */
  protected final QbftProtocolSchedule protocolSchedule;

  private final NodeKey nodeKey;
  private final Address localAddress;
  private final MessageFactory messageFactory; // used only to create stored local msgs
  private final QbftMessageTransmitter transmitter;

  private final QbftBlockHeader parentHeader;

  /**
   * Instantiates a new Qbft round.
   *
   * @param roundState the round state
   * @param blockCreator the block creator
   * @param blockInterface the block interface
   * @param protocolSchedule the protocol schedule
   * @param observers the observers
   * @param nodeKey the node key
   * @param localAddress the local address
   * @param messageFactory the message factory
   * @param transmitter the transmitter
   * @param roundTimer the round timer
   * @param parentHeader the parent header
   */
  public QbftRound(
      final RoundState roundState,
      final QbftBlockCreator blockCreator,
      final QbftBlockInterface blockInterface,
      final QbftProtocolSchedule protocolSchedule,
      final Subscribers<QbftMinedBlockObserver> observers,
      final NodeKey nodeKey,
      final Address localAddress,
      final MessageFactory messageFactory,
      final QbftMessageTransmitter transmitter,
      final RoundTimer roundTimer,
      final QbftBlockHeader parentHeader) {
    this.roundState = roundState;
    this.blockCreator = blockCreator;
    this.blockInterface = blockInterface;
    this.protocolSchedule = protocolSchedule;
    this.observers = observers;
    this.nodeKey = nodeKey;
    this.localAddress = localAddress;
    this.messageFactory = messageFactory;
    this.transmitter = transmitter;
    this.parentHeader = parentHeader;
    roundTimer.startTimer(getRoundIdentifier());
  }

  /**
   * Gets round identifier.
   *
   * @return the round identifier
   */
  public ConsensusRoundIdentifier getRoundIdentifier() {
    return roundState.getRoundIdentifier();
  }

  /**
   * Gets round state.
   *
   * @return the round state
   */
  public RoundState getRoundState() {
    return roundState;
  }

  /**
   * Create a block
   *
   * @param headerTimeStampSeconds of the block
   * @return a block creation result
   */
  public BlockCreationResult createBlock(final long headerTimeStampSeconds) {
    LOG.debug("Creating proposed block. round={}", roundState.getRoundIdentifier());
    return blockCreator.createBlock(headerTimeStampSeconds, this.parentHeader);
  }

  /**
   * Start round with.
   *
   * @param roundChangeArtifacts the round change artifacts
   * @param headerTimestamp the header timestamp
   */
  public void startRoundWith(
      final RoundChangeArtifacts roundChangeArtifacts, final long headerTimestamp) {
    final Optional<PreparedCertificate> bestPreparedCertificate =
        roundChangeArtifacts.getBestPreparedPeer();

    final QbftBlock blockToPublish;
    final Optional<BlockAccessList> blockAccessList;
    if (bestPreparedCertificate.isEmpty()) {
      LOG.debug("Sending proposal with new block. round={}", roundState.getRoundIdentifier());
      final BlockCreationResult blockCreationResult =
          blockCreator.createBlock(headerTimestamp, this.parentHeader);
      blockToPublish = blockCreationResult.block();
      blockAccessList = blockCreationResult.blockAccessList();
    } else {
      LOG.debug(
          "Sending proposal from PreparedCertificate. round={}", roundState.getRoundIdentifier());
      QbftBlock preparedBlock = bestPreparedCertificate.get().getBlock();
      blockToPublish =
          blockInterface.replaceRoundAndProposerForProposalBlock(
              preparedBlock, roundState.getRoundIdentifier().getRoundNumber(), localAddress);
      blockAccessList = bestPreparedCertificate.get().getBlockAccessList();
    }

    LOG.debug(" proposal - new/prepared block hash : {}", blockToPublish.getHash());

    updateStateWithProposalAndTransmit(
        blockToPublish,
        blockAccessList,
        roundChangeArtifacts.getRoundChanges(),
        bestPreparedCertificate.map(PreparedCertificate::getPrepares).orElse(emptyList()));
  }

  /**
   * Update state with proposal and transmit.
   *
   * @param block the block
   * @param blockAccessList optional block access list
   * @param roundChanges the round changes
   * @param prepares the prepares
   */
  protected void updateStateWithProposalAndTransmit(
      final QbftBlock block,
      final Optional<BlockAccessList> blockAccessList,
      final List<SignedData<RoundChangePayload>> roundChanges,
      final List<SignedData<PreparePayload>> prepares) {
    final Proposal proposal;
    try {
      proposal =
          messageFactory.createProposal(
              getRoundIdentifier(), block, blockAccessList, roundChanges, prepares);
    } catch (final SecurityModuleException e) {
      LOG.warn("Failed to create a signed Proposal, waiting for next round.", e);
      return;
    }

    transmitter.multicastProposal(
        proposal.getRoundIdentifier(),
        proposal.getSignedPayload().getPayload().getProposedBlock(),
        proposal.getBlockAccessList(),
        roundChanges,
        prepares);
    if (updateStateWithProposedBlock(proposal)) {
      sendPrepare(block);
    }
  }

  /**
   * Handle proposal message.
   *
   * @param msg the msg
   */
  public void handleProposalMessage(final Proposal msg) {
    LOG.debug(
        "Received a proposal message. round={}. author={}",
        roundState.getRoundIdentifier(),
        msg.getAuthor());
    final QbftBlock block = msg.getSignedPayload().getPayload().getProposedBlock();
    if (updateStateWithProposedBlock(msg)) {
      sendPrepare(block);
    }
  }

  private void sendPrepare(final QbftBlock block) {
    LOG.debug("Sending prepare message. round={}", roundState.getRoundIdentifier());
    try {
      final Prepare localPrepareMessage =
          messageFactory.createPrepare(getRoundIdentifier(), block.getHash());
      peerIsPrepared(localPrepareMessage);
      transmitter.multicastPrepare(
          localPrepareMessage.getRoundIdentifier(), localPrepareMessage.getDigest());
    } catch (final SecurityModuleException e) {
      LOG.warn("Failed to create a signed Prepare; {}", e.getMessage());
    }
  }

  /**
   * Handle prepare message.
   *
   * @param msg the msg
   */
  public void handlePrepareMessage(final Prepare msg) {
    LOG.debug(
        "Received a prepare message. round={}. author={}",
        roundState.getRoundIdentifier(),
        msg.getAuthor());
    peerIsPrepared(msg);
  }

  /**
   * Handle commit message.
   *
   * @param msg the msg
   */
  public void handleCommitMessage(final Commit msg) {
    LOG.debug(
        "Received a commit message. round={}. author={}",
        roundState.getRoundIdentifier(),
        msg.getAuthor());
    peerIsCommitted(msg);
  }

  /**
   * Construct prepared certificate.
   *
   * @return the optional PreparedCertificate
   */
  public Optional<PreparedCertificate> constructPreparedCertificate() {
    return roundState.constructPreparedCertificate();
  }

  private boolean updateStateWithProposedBlock(final Proposal msg) {
    final boolean wasPrepared = roundState.isPrepared();
    final boolean wasCommitted = roundState.isCommitted();
    final boolean blockAccepted = roundState.setProposedBlock(msg);

    if (blockAccepted) {
      final QbftBlock block = roundState.getProposedBlock().get();
      final SECPSignature commitSeal;
      try {
        commitSeal = createCommitSeal(block);
      } catch (final SecurityModuleException e) {
        LOG.warn("Failed to construct commit seal; {}", e.getMessage());
        return true;
      }

      // There are times handling a proposed block is enough to enter prepared.
      if (wasPrepared != roundState.isPrepared()) {
        LOG.debug("Sending commit message. round={}", roundState.getRoundIdentifier());
        transmitter.multicastCommit(getRoundIdentifier(), block.getHash(), commitSeal);
      }

      // can automatically add _our_ commit message to the roundState
      // cannot create a prepare message here, as it may be _our_ proposal, and thus we cannot also
      // prepare
      try {
        final Commit localCommitMessage =
            messageFactory.createCommit(
                roundState.getRoundIdentifier(), msg.getBlock().getHash(), commitSeal);
        roundState.addCommitMessage(localCommitMessage);
      } catch (final SecurityModuleException e) {
        LOG.warn("Failed to create signed Commit message; {}", e.getMessage());
        return true;
      }

      // It is possible sufficient commit seals are now available and the block should be imported
      if (wasCommitted != roundState.isCommitted()) {
        importBlockToChain();
      }
    }

    return blockAccepted;
  }

  private void peerIsPrepared(final Prepare msg) {
    final boolean wasPrepared = roundState.isPrepared();
    roundState.addPrepareMessage(msg);
    if (wasPrepared != roundState.isPrepared()) {
      LOG.debug("Sending commit message. round={}", roundState.getRoundIdentifier());
      final QbftBlock block = roundState.getProposedBlock().get();
      try {
        transmitter.multicastCommit(getRoundIdentifier(), block.getHash(), createCommitSeal(block));
        // Note: the local-node's commit message was added to RoundState on block acceptance
        // and thus does not need to be done again here.
      } catch (final SecurityModuleException e) {
        LOG.warn("Failed to construct a commit seal: {}", e.getMessage());
      }
    }
  }

  private void peerIsCommitted(final Commit msg) {
    final boolean wasCommitted = roundState.isCommitted();
    roundState.addCommitMessage(msg);
    if (wasCommitted != roundState.isCommitted()) {
      importBlockToChain();
    }
  }

  private void importBlockToChain() {

    final QbftBlock blockToImport =
        blockCreator.createSealedBlock(
            roundState.getProposedBlock().get(),
            roundState.getRoundIdentifier().getRoundNumber(),
            roundState.getCommitSeals());

    final long blockNumber = blockToImport.getHeader().getNumber();
    if (getRoundIdentifier().getRoundNumber() > 0) {
      LOG.info(
          "Importing proposed block to chain. round={}, hash={}",
          getRoundIdentifier(),
          blockToImport.getHash());
    } else {
      LOG.debug(
          "Importing proposed block to chain. round={}, hash={}",
          getRoundIdentifier(),
          blockToImport.getHash());
    }

    final QbftBlockImporter blockImporter =
        protocolSchedule.getBlockImporter(blockToImport.getHeader());
    final boolean result =
        blockImporter.importBlock(blockToImport, roundState.getProposedBlockAccessList());
    if (!result) {
      LOG.error(
          "Failed to import proposed block to chain. block={} blockHeader={}",
          blockNumber,
          blockToImport.getHeader());
    } else {
      notifyNewBlockListeners(blockToImport);
    }
  }

  private SECPSignature createCommitSeal(final QbftBlock block) {
    final QbftBlock commitBlock = createCommitBlock(block);
    final Hash commitHash = commitBlock.getHash();
    return nodeKey.sign(Bytes32.wrap(commitHash.getBytes()));
  }

  private QbftBlock createCommitBlock(final QbftBlock block) {
    return blockInterface.replaceRoundForCommitBlock(block, getRoundIdentifier().getRoundNumber());
  }

  private void notifyNewBlockListeners(final QbftBlock block) {
    observers.forEach(obs -> obs.blockMined(block));
  }
}
