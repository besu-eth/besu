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

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Commit;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.validation.MessageValidator;
import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Round state holds all the messages for a given round and tracks whether quorum has been
 * reached for the round to be prepared or committed.
 */
public class RoundState {
  private static final Logger LOG = LoggerFactory.getLogger(RoundState.class);

  private final ConsensusRoundIdentifier roundIdentifier;
  private final MessageValidator validator;
  private final long quorum;

  private Optional<Proposal> proposalMessage = Optional.empty();

  // Must track the actual Prepare message, not just the sender, as these may need to be reused
  // to send out in a PrepareCertificate.
  //
  // Synchronized maps: writes (e.g. addCommitMessage) happen on the BFT processor thread while
  // reads that iterate (e.g. getCommitSeals from block import) can run on another thread. A plain
  // LinkedHashMap's fail-fast iterator throws ConcurrentModificationException in that case
  // (see #10806). Insertion order is preserved; every iteration below holds the map's monitor.
  private final Map<Address, Prepare> prepareMessages =
      Collections.synchronizedMap(new LinkedHashMap<>());
  private final Map<Address, Commit> commitMessages =
      Collections.synchronizedMap(new LinkedHashMap<>());

  private boolean prepared = false;
  private boolean committed = false;

  /**
   * Instantiates a new Round state.
   *
   * @param roundIdentifier the round identifier
   * @param quorum the quorum
   * @param validator the validator
   */
  public RoundState(
      final ConsensusRoundIdentifier roundIdentifier,
      final int quorum,
      final MessageValidator validator) {
    this.roundIdentifier = roundIdentifier;
    this.quorum = quorum;
    this.validator = validator;
  }

  /**
   * Gets round identifier.
   *
   * @return the round identifier
   */
  public ConsensusRoundIdentifier getRoundIdentifier() {
    return roundIdentifier;
  }

  /**
   * Gets the message validator.
   *
   * @return the message validator
   */
  public MessageValidator getValidator() {
    return validator;
  }

  /**
   * Sets proposed block.
   *
   * @param msg the Proposal payload msg
   * @return the proposed block
   */
  public boolean setProposedBlock(final Proposal msg) {

    if (proposalMessage.isEmpty()) {
      if (validator.validateProposal(msg)) {
        proposalMessage = Optional.of(msg);
        synchronized (prepareMessages) {
          prepareMessages.entrySet().removeIf(e -> !validator.validatePrepare(e.getValue()));
        }
        synchronized (commitMessages) {
          commitMessages.entrySet().removeIf(e -> !validator.validateCommit(e.getValue()));
        }
        updateState();
        return true;
      }
    }

    return false;
  }

  /**
   * Add prepare message.
   *
   * @param msg the msg
   */
  public void addPrepareMessage(final Prepare msg) {
    if (proposalMessage.isEmpty() || validator.validatePrepare(msg)) {
      prepareMessages.putIfAbsent(msg.getAuthor(), msg);
      LOG.trace("Round state added prepare message prepare={}", msg);
    }
    updateState();
  }

  /**
   * Add commit message.
   *
   * @param msg the msg
   */
  public void addCommitMessage(final Commit msg) {
    if (proposalMessage.isEmpty() || validator.validateCommit(msg)) {
      commitMessages.putIfAbsent(msg.getAuthor(), msg);
      LOG.trace("Round state added commit message commit={}", msg);
    }

    updateState();
  }

  private void updateState() {
    prepared = (prepareMessages.size() >= quorum) && proposalMessage.isPresent();
    committed = (commitMessages.size() >= quorum) && proposalMessage.isPresent();
    LOG.trace(
        "Round state updated prepared={} committed={} preparedQuorum={}/{} committedQuorum={}/{}",
        prepared,
        committed,
        prepareMessages.size(),
        quorum,
        commitMessages.size(),
        quorum);
  }

  /**
   * Gets proposed block.
   *
   * @return the proposed block
   */
  public Optional<QbftBlock> getProposedBlock() {
    return proposalMessage.map(p -> p.getSignedPayload().getPayload().getProposedBlock());
  }

  /**
   * Gets proposed block access list.
   *
   * @return the block access list
   */
  public Optional<BlockAccessList> getProposedBlockAccessList() {
    return proposalMessage.flatMap(Proposal::getBlockAccessList);
  }

  /**
   * Is prepared.
   *
   * @return the boolean
   */
  public boolean isPrepared() {
    return prepared;
  }

  /**
   * Is committed.
   *
   * @return the boolean
   */
  public boolean isCommitted() {
    return committed;
  }

  /**
   * Gets commit seals.
   *
   * @return the commit seals
   */
  public Collection<SECPSignature> getCommitSeals() {
    synchronized (commitMessages) {
      return commitMessages.values().stream()
          .map(cp -> cp.getSignedPayload().getPayload().getCommitSeal())
          .collect(Collectors.toList());
    }
  }

  /**
   * Construct prepared certificate.
   *
   * @return the optional prepared certificate
   */
  public Optional<PreparedCertificate> constructPreparedCertificate() {
    if (isPrepared()) {
      final List<SignedData<PreparePayload>> preparePayloads;
      synchronized (prepareMessages) {
        preparePayloads =
            prepareMessages.values().stream()
                .map(Prepare::getSignedPayload)
                .collect(Collectors.toList());
      }
      return Optional.of(
          new PreparedCertificate(
              proposalMessage.get().getSignedPayload().getPayload().getProposedBlock(),
              preparePayloads,
              roundIdentifier.getRoundNumber(),
              proposalMessage.get().getBlockAccessList()));
    }
    return Optional.empty();
  }
}
