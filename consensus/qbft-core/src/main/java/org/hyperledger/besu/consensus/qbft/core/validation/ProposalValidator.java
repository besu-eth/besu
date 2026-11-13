/*
 * Copyright 2020 ConsenSys AG.
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
package org.hyperledger.besu.consensus.qbft.core.validation;

import static org.hyperledger.besu.consensus.common.bft.validation.ValidationHelpers.hasDuplicateAuthors;
import static org.hyperledger.besu.consensus.common.bft.validation.ValidationHelpers.hasSufficientEntries;

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.blockcreation.ProposerSelector;
import org.hyperledger.besu.consensus.common.bft.payload.Payload;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockInterface;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Proposal validator. */
public class ProposalValidator {

  private static final Logger LOG = LoggerFactory.getLogger(ProposalValidator.class);
  private static final String ERROR_PREFIX = "Invalid Proposal Payload";

  private final QbftBlockInterface blockInterface;
  private final QbftProtocolSchedule protocolSchedule;
  private final int quorumMessageCount;
  private final Collection<Address> validators;
  private final ConsensusRoundIdentifier roundIdentifier;
  private final ProposerSelector proposerSelector;

  /**
   * Instantiates a new Proposal validator.
   *
   * @param blockInterface the block interface
   * @param protocolSchedule the protocol schedule
   * @param quorumMessageCount the quorum message count
   * @param validators the validators
   * @param roundIdentifier the round identifier
   * @param proposerSelector the proposer selector
   */
  public ProposalValidator(
      final QbftBlockInterface blockInterface,
      final QbftProtocolSchedule protocolSchedule,
      final int quorumMessageCount,
      final Collection<Address> validators,
      final ConsensusRoundIdentifier roundIdentifier,
      final ProposerSelector proposerSelector) {
    this.blockInterface = blockInterface;
    this.protocolSchedule = protocolSchedule;
    this.quorumMessageCount = quorumMessageCount;
    this.validators = validators;
    this.roundIdentifier = roundIdentifier;
    this.proposerSelector = proposerSelector;
  }

  /**
   * Validate.
   *
   * @param msg the Proposal msg
   * @return the boolean
   */
  public boolean validate(final Proposal msg) {
    return validate(msg, true);
  }

  /**
   * Validate without block validation.
   *
   * @param msg the Proposal msg
   * @return the boolean
   */
  public boolean validateWithoutBlockValidation(final Proposal msg) {
    return validate(msg, false);
  }

  /**
   * Validate with optional block validation.
   *
   * @param msg the Proposal msg
   * @param validateBlock whether to validate the block
   * @return the boolean
   */
  private boolean validate(final Proposal msg, final boolean validateBlock) {
    final QbftBlockValidator blockValidator =
        protocolSchedule.getBlockValidator(msg.getBlock().getHeader());

    final Address expectedProposer = proposerSelector.selectProposerForRound(roundIdentifier);
    final ProposalPayloadValidator payloadValidator =
        new ProposalPayloadValidator(expectedProposer, roundIdentifier, blockValidator);

    final boolean payloadValid =
        validateBlock
            ? payloadValidator.validate(msg.getSignedPayload())
            : payloadValidator.validateWithoutBlockValidation(msg.getSignedPayload());

    if (!payloadValid) {
      LOG.info("{}: invalid proposal payload in proposal message", ERROR_PREFIX);
      return false;
    }

    if (!validateProposalAndRoundChangeAreConsistent(msg)) {
      return false;
    }

    return true;
  }

  private boolean validateProposalAndRoundChangeAreConsistent(final Proposal proposal) {
    final ConsensusRoundIdentifier proposalRoundIdentifier = proposal.getRoundIdentifier();

    if (proposalRoundIdentifier.getRoundNumber() == 0) {
      if (!validateRoundZeroProposalHasNoRoundChangesOrPrepares(proposal)) {
        return false;
      }

      return validateBlockCoinbaseMatchesMsgAuthor(proposal);
    } else {

      if (!validateRoundChanges(proposal, proposal.getRoundChanges())) {
        LOG.info("{}: failed to validate piggy-backed round change payloads", ERROR_PREFIX);
        return false;
      }

      // The RoundChangePayloadValidator ensures the PreparedRound is less than targetRound
      // therefore, no need to validate that here.
      final Optional<SignedData<RoundChangePayload>> roundChangeWithLatestPreparedRound =
          getRoundChangeWithLatestPreparedRound(proposal.getRoundChanges());

      if (roundChangeWithLatestPreparedRound.isPresent()) {
        SignedData<RoundChangePayload> roundChangePayloadSignedData =
            roundChangeWithLatestPreparedRound.get();
        final PreparedRoundMetadata metadata =
            roundChangePayloadSignedData.getPayload().getPreparedRoundMetadata().get();

        LOG.debug(
            "Prepared Metadata blockhash : {}, proposal blockhash: {}, prepared round in message: {}, proposal round in message: {}",
            metadata.getPreparedBlockHash(),
            proposal.getBlock().getHash(),
            metadata.getPreparedRound(),
            proposal.getRoundIdentifier().getRoundNumber());

        // The Hash in the roundchange/proposals is NOT the same as the value in the
        // prepares/roundchanges
        // as said payloads reference the block with an OLD round number in it - therefore, need
        // to create a block with the old round in it, then re-calc expected hash
        // Need to check that if we substitute the LatestPrepareCert round number into the supplied
        // block that we get the SAME hash as PreparedCert.
        final Address proposerForOldRound =
            proposerSelector.selectProposerForRound(
                new ConsensusRoundIdentifier(
                    roundIdentifier.getSequenceNumber(), metadata.getPreparedRound()));
        final QbftBlock currentBlockWithOldRound =
            blockInterface.replaceRoundAndProposerForProposalBlock(
                proposal.getBlock(), metadata.getPreparedRound(), proposerForOldRound);

        final Hash expectedPriorBlockHash = currentBlockWithOldRound.getHash();

        if (!metadata.getPreparedBlockHash().equals(expectedPriorBlockHash)) {
          LOG.info(
              "{}: Latest Prepared Metadata blockhash does not align with proposed block. Expected: {}, Actual: {}",
              ERROR_PREFIX,
              expectedPriorBlockHash,
              metadata.getPreparedBlockHash());
          return false;
        }

        // validate the prepares
        if (!validatePrepares(
            metadata, proposal.getRoundIdentifier().getSequenceNumber(), proposal.getPrepares())) {
          LOG.info("{}: Piggy-backed prepares failed validation", ERROR_PREFIX);
          return false;
        }
      } else {
        // no one prepared, so prepares should be empty
        if (!proposal.getPrepares().isEmpty()) {
          LOG.info("{}: No PreparedMetadata exists, so prepare list must be empty", ERROR_PREFIX);
          return false;
        }

        return validateBlockCoinbaseMatchesMsgAuthor(proposal);
      }

      return true;
    }
  }

  private boolean validateRoundZeroProposalHasNoRoundChangesOrPrepares(final Proposal proposal) {
    if ((!proposal.getRoundChanges().isEmpty()) || !proposal.getPrepares().isEmpty()) {
      LOG.info("{}: round-0 proposal must not contain any prepares or roundchanges", ERROR_PREFIX);
      return false;
    }

    return true;
  }

  private boolean validateRoundChanges(
      final Proposal proposal, final List<SignedData<RoundChangePayload>> roundChanges) {

    if (hasDuplicateAuthors(roundChanges)) {
      LOG.info("{}: multiple round changes from the same author.", ERROR_PREFIX);
      return false;
    }

    if (!hasSufficientEntries(roundChanges, quorumMessageCount)) {
      LOG.info("{}: Insufficient round changes for proposal", ERROR_PREFIX);
      return false;
    }

    if (!metadataIsConsistentAcrossRoundChanges(roundChanges)) {
      return false;
    }

    final RoundChangePayloadValidator roundChangePayloadValidator =
        new RoundChangePayloadValidator(validators, roundIdentifier.getSequenceNumber());

    if (!roundChanges.stream().allMatch(roundChangePayloadValidator::validate)) {
      LOG.info("{}: invalid proposal, round changes did not pass validation", ERROR_PREFIX);
      return false;
    }

    // This is required as the RoundChangePayloadValidator only checks height, not round.
    if (!allMessagesTargetRound(roundChanges, proposal.getRoundIdentifier())) {
      LOG.info("{}: not all roundChange payloads target the proposal round.", ERROR_PREFIX);
      return false;
    }

    return true;
  }

  private boolean validatePrepares(
      final PreparedRoundMetadata metaData,
      final long currentHeight,
      final List<SignedData<PreparePayload>> prepares) {

    if (hasDuplicateAuthors(prepares)) {
      LOG.info("{}}: multiple prepares from the same author.", ERROR_PREFIX);
      return false;
    }

    if (!hasSufficientEntries(prepares, quorumMessageCount)) {
      LOG.info("{}: Insufficient prepares for proposal", ERROR_PREFIX);
      return false;
    }

    final ConsensusRoundIdentifier preparedRoundIdentifier =
        new ConsensusRoundIdentifier(currentHeight, metaData.getPreparedRound());

    final PrepareValidator prepareValidator =
        new PrepareValidator(validators, preparedRoundIdentifier, metaData.getPreparedBlockHash());

    if (!prepares.stream().allMatch(prepareValidator::validate)) {
      LOG.info("{}: Prepare failed validation", ERROR_PREFIX);
      return false;
    }

    return true;
  }

  private Optional<SignedData<RoundChangePayload>> getRoundChangeWithLatestPreparedRound(
      final List<SignedData<RoundChangePayload>> roundChanges) {

    final Comparator<SignedData<RoundChangePayload>> preparedRoundComparator =
        (o1, o2) -> {
          if (o1.getPayload().getPreparedRoundMetadata().isEmpty()) {
            return -1;
          }
          if (o2.getPayload().getPreparedRoundMetadata().isEmpty()) {
            return 1;
          }

          int o1Round = o1.getPayload().getPreparedRoundMetadata().get().getPreparedRound();
          int o2Round = o2.getPayload().getPreparedRoundMetadata().get().getPreparedRound();

          return Integer.compare(o1Round, o2Round);
        };

    return roundChanges.stream()
        .max(preparedRoundComparator)
        .flatMap(rc -> rc.getPayload().getPreparedRoundMetadata().map(metadata -> rc));
  }

  private boolean metadataIsConsistentAcrossRoundChanges(
      final List<SignedData<RoundChangePayload>> roundChanges) {
    final List<PreparedRoundMetadata> distinctMetadatas =
        roundChanges.stream()
            .map(rc -> rc.getPayload().getPreparedRoundMetadata())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .distinct()
            .toList();

    final List<Integer> preparedRounds =
        distinctMetadatas.stream().map(PreparedRoundMetadata::getPreparedRound).toList();

    for (final Integer preparedRound : preparedRounds) {
      if (distinctMetadatas.stream().filter(dm -> dm.getPreparedRound() == preparedRound).count()
          > 1) {
        LOG.info("{}: Roundchanges have different prepared metadata for same round", ERROR_PREFIX);
        return false;
      }
    }
    return true;
  }

  private boolean validateBlockCoinbaseMatchesMsgAuthor(final Proposal msg) {
    if (!msg.getBlock().getHeader().getCoinbase().equals(msg.getAuthor())) {
      LOG.info("{}: block coinbase does not match the proposer's address", ERROR_PREFIX);
      return false;
    }
    return true;
  }

  /**
   * All messages target round boolean.
   *
   * @param <T> the type parameter
   * @param payloads the payloads
   * @param requiredRound the required round
   * @return the boolean
   */
  public static <T extends Payload> boolean allMessagesTargetRound(
      final Collection<SignedData<T>> payloads, final ConsensusRoundIdentifier requiredRound) {
    return payloads.stream()
        .allMatch(payload -> payload.getPayload().getRoundIdentifier().equals(requiredRound));
  }
}
