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
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.qbft.core.validation.RoundChangeMessageValidator;
import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for handling all RoundChange messages received for a given block height
 * (theoretically, RoundChange messages for a older height should have been previously discarded,
 * and messages for a future round should have been buffered).
 *
 * <p>If enough RoundChange messages all targeting a given round are received (and this node is the
 * proposer for said round) - a proposal message is sent, and a new round should be started by the
 * controlling class.
 */
public class RoundChangeManager {

  /** The type Round change status. */
  public static class RoundChangeStatus {

    private final long quorum;

    /** The Received messages. */
    // Store only 1 round change per round per validator
    @VisibleForTesting final Map<Address, RoundChange> receivedMessages = Maps.newLinkedHashMap();

    private boolean actioned = false;

    /**
     * Instantiates a new Round change status.
     *
     * @param quorum the quorum
     */
    public RoundChangeStatus(final long quorum) {
      this.quorum = quorum;
    }

    /**
     * Add message.
     *
     * @param msg the msg
     */
    public void addMessage(final RoundChange msg) {
      if (!actioned) {
        receivedMessages.putIfAbsent(msg.getAuthor(), msg);
      }
    }

    /**
     * Is Round change ready.
     *
     * @return the boolean
     */
    public boolean roundChangeQuorumReceived() {
      return receivedMessages.size() >= quorum && !actioned;
    }

    /**
     * Create round change certificate collection.
     *
     * @return the collection
     */
    public Collection<RoundChange> createRoundChangeCertificate() {
      if (roundChangeQuorumReceived()) {
        actioned = true;
        return receivedMessages.values();
      } else {
        throw new IllegalStateException("Unable to create RoundChangeCertificate at this time.");
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RoundChangeManager.class);

  /** The Round change cache. */
  @VisibleForTesting
  final Map<ConsensusRoundIdentifier, RoundChangeStatus> roundChangeCache = Maps.newHashMap();

  /** A summary of the latest round each validator is on, for diagnostic purposes only */
  private final Map<Address, ConsensusRoundIdentifier> roundSummary = Maps.newHashMap();

  private final long quorum;
  private long rcQuorum;
  private final RoundChangeMessageValidator roundChangeMessageValidator;
  private final Address localAddress;

  /**
   * Instantiates a new Round change manager.
   *
   * @param quorum the quorum
   * @param roundChangeMessageValidator the round change message validator
   * @param localAddress this node's address
   */
  public RoundChangeManager(
      final long quorum,
      final RoundChangeMessageValidator roundChangeMessageValidator,
      final Address localAddress) {
    this.quorum = quorum;
    this.roundChangeMessageValidator = roundChangeMessageValidator;
    this.localAddress = localAddress;
  }

  /**
   * Instantiates a new Round change manager.
   *
   * @param quorum the quorum
   * @param rcQuorum quorum for round change messages
   * @param roundChangeMessageValidator the round change message validator
   * @param localAddress this node's address
   */
  public RoundChangeManager(
      final long quorum,
      final long rcQuorum,
      final RoundChangeMessageValidator roundChangeMessageValidator,
      final Address localAddress) {
    this(quorum, roundChangeMessageValidator, localAddress);
    this.rcQuorum = rcQuorum;
  }

  /**
   * Store the latest round for a node, and if chain is stalled log a summary of which round each
   * address is on
   *
   * @param message the round-change message that has just been received
   */
  public void storeAndLogRoundChangeSummary(final RoundChange message) {
    if (!isMessageValid(message)) {
      LOG.info("RoundChange message is invalid .");
      return;
    }
    storeAndLogValidatedRoundChangeSummary(message);
  }

  /**
   * As {@link #storeAndLogRoundChangeSummary}, for a message the caller has already validated.
   *
   * @param message the round-change message that has just been received and validated
   */
  public void storeAndLogValidatedRoundChangeSummary(final RoundChange message) {
    roundSummary.put(message.getAuthor(), message.getRoundIdentifier());
    if (roundChangeCache.keySet().stream()
            .findFirst()
            .orElse(new ConsensusRoundIdentifier(0, 0))
            .getRoundNumber()
        >= 2) {
      LOG.info("BFT round summary (quorum = {})", quorum);
      for (Map.Entry<Address, ConsensusRoundIdentifier> nextEntry : roundSummary.entrySet()) {
        LOG.info(
            "Address: {}  Round: {} {}",
            nextEntry.getKey(),
            nextEntry.getValue().getRoundNumber(),
            nextEntry.getKey().equals(localAddress) ? "(Local node)" : "");
      }
    }
  }

  /**
   * Checks if a quorum of round change messages has been received for a round higher than the
   * current round
   *
   * @param currentRoundIdentifier the current round identifier
   * @return the next higher round number if quorum is reached, otherwise empty Optional
   */
  public Optional<Integer> futureRCQuorumReceived(
      final ConsensusRoundIdentifier currentRoundIdentifier) {
    // Iterate through elements of round summary, identify ones with round number higher than
    // current,
    // tracking minimum of those and return the next higher round number if quorum is reached

    // Filter out entries with round number greater than current round
    // and collect their round numbers
    Map<Address, Integer> higherRounds =
        roundSummary.entrySet().stream()
            .filter(entry -> isAFutureRound(entry.getValue(), currentRoundIdentifier))
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getRoundNumber()));

    LOG.debug("Higher rounds size ={} rcquorum = {}", higherRounds.size(), rcQuorum);

    // Check if we have at least f + 1 validators at higher rounds
    if (higherRounds.size() >= rcQuorum) {
      // Find the minimum round that is greater than the current round
      return Optional.of(higherRounds.values().stream().min(Integer::compareTo).orElseThrow());
    }

    // If quorum is not reached, return empty Optional
    return Optional.empty();
  }

  /**
   * Adds the round message to this manager and return a certificate if it passes the threshold
   *
   * @param msg The signed round change message to add
   * @return Empty if the round change threshold hasn't been hit, otherwise a round change
   *     certificate
   */
  public Optional<Collection<RoundChange>> appendRoundChangeMessage(final RoundChange msg) {

    if (!isMessageValid(msg)) {
      LOG.info("RoundChange message was invalid.");
      return Optional.empty();
    }

    return appendValidatedRoundChangeMessage(msg);
  }

  /**
   * Adds an already-validated round change message to this manager and returns a certificate if it
   * passes the threshold.
   *
   * <p>Validating a RoundChange that carries a block executes every transaction in that block, so a
   * caller that has already called {@link #isMessageValid} for this message uses this variant to
   * avoid paying that cost a second time. Callers that have not validated the message must use
   * {@link #appendRoundChangeMessage} instead.
   *
   * @param msg the signed round change message to add, which the caller has already validated
   * @return empty if the round change threshold hasn't been hit, otherwise a round change
   *     certificate
   */
  public Optional<Collection<RoundChange>> appendValidatedRoundChangeMessage(
      final RoundChange msg) {

    final RoundChangeStatus roundChangeStatus = storeRoundChangeMessage(msg);

    if (roundChangeStatus.roundChangeQuorumReceived()) {
      return Optional.of(roundChangeStatus.createRoundChangeCertificate());
    }

    return Optional.empty();
  }

  /**
   * Validates a round change message. For a message carrying a proposed block this executes the
   * block, so the verdict is worth reusing rather than recomputing.
   *
   * @param msg the round change message to validate
   * @return true if the message is valid
   */
  public boolean isMessageValid(final RoundChange msg) {
    return roundChangeMessageValidator.validate(msg);
  }

  private RoundChangeStatus storeRoundChangeMessage(final RoundChange msg) {
    final ConsensusRoundIdentifier msgTargetRound = msg.getRoundIdentifier();

    final RoundChangeStatus roundChangeStatus =
        roundChangeCache.computeIfAbsent(msgTargetRound, ignored -> new RoundChangeStatus(quorum));

    roundChangeStatus.addMessage(msg);

    return roundChangeStatus;
  }

  /**
   * Clears old rounds from storage that have been superseded by a given round
   *
   * @param completedRoundIdentifier round identifier that has been identified as superseded
   */
  public void discardRoundsPriorTo(final ConsensusRoundIdentifier completedRoundIdentifier) {
    roundChangeCache.keySet().removeIf(k -> isAnEarlierRound(k, completedRoundIdentifier));
  }

  private boolean isAnEarlierRound(
      final ConsensusRoundIdentifier left, final ConsensusRoundIdentifier right) {
    return left.getRoundNumber() < right.getRoundNumber();
  }

  private boolean isAFutureRound(
      final ConsensusRoundIdentifier left, final ConsensusRoundIdentifier right) {
    return left.getRoundNumber() > right.getRoundNumber();
  }

  /**
   * Gets the round change message validator.
   *
   * @return the round change message validator
   */
  public RoundChangeMessageValidator getRoundChangeMessageValidator() {
    return roundChangeMessageValidator;
  }
}
