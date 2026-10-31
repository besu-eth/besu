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
package org.hyperledger.besu.consensus.ibft.validation;

import org.hyperledger.besu.consensus.common.bft.BftBlockInterface;
import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.consensus.ibft.payload.RoundChangeCertificate;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Message validator. */
public class MessageValidator {

  private static final Logger LOG = LoggerFactory.getLogger(MessageValidator.class);

  private final SignedDataValidator signedDataValidator;
  private final ProposalBlockConsistencyValidator proposalConsistencyValidator;
  private final ProtocolSchedule protocolSchedule;
  private final ProtocolContext protocolContext;
  private final RoundChangeCertificateValidator roundChangeCertificateValidator;

  /**
   * Instantiates a new Message validator.
   *
   * @param signedDataValidator the signed data validator
   * @param proposalConsistencyValidator the proposal consistency validator
   * @param protocolSchedule the protocol schedule
   * @param protocolContext the protocol context
   * @param roundChangeCertificateValidator the round change certificate validator
   */
  public MessageValidator(
      final SignedDataValidator signedDataValidator,
      final ProposalBlockConsistencyValidator proposalConsistencyValidator,
      final ProtocolContext protocolContext,
      final ProtocolSchedule protocolSchedule,
      final RoundChangeCertificateValidator roundChangeCertificateValidator) {
    this.signedDataValidator = signedDataValidator;
    this.proposalConsistencyValidator = proposalConsistencyValidator;
    this.protocolSchedule = protocolSchedule;
    this.protocolContext = protocolContext;
    this.roundChangeCertificateValidator = roundChangeCertificateValidator;
  }

  /**
   * Validate proposal.
   *
   * @param msg the msg
   * @return the boolean
   */
  public boolean validateProposal(final Proposal msg) {

    if (!signedDataValidator.validateProposal(msg.getSignedPayload())) {
      LOG.info("Illegal Proposal message, embedded signed data failed validation");
      return false;
    }

    // We want to validate the block but not persist it yet as it's just a proposal. If it turns
    // out to be an accepted block it will be persisted at block import time
    if (!validateBlockWithoutPersisting(msg.getBlock(), msg.getBlockAccessList())) {
      return false;
    }

    if (!validateProposalAndRoundChangeAreConsistent(msg)) {
      LOG.info("Illegal Proposal message, embedded roundChange does not match proposal.");
      return false;
    }

    final BftBlockInterface blockInterface =
        protocolContext.getConsensusContext(BftContext.class).getBlockInterface();
    return proposalConsistencyValidator.validateProposalMatchesBlock(
        msg.getSignedPayload(), msg.getBlock(), blockInterface);
  }

  private boolean validateBlockWithoutPersisting(
      final Block block, final Optional<BlockAccessList> blockAccessList) {

    final BlockValidator blockValidator =
        protocolSchedule.getByBlockHeader(block.getHeader()).getBlockValidator();

    final var validationResult =
        blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            false);

    if (validationResult.isFailed()) {
      LOG.info(
          "Invalid Proposal message, block did not pass validation. Reason {}",
          validationResult.errorMessage);
      return false;
    }

    return true;
  }

  private boolean validateProposalAndRoundChangeAreConsistent(final Proposal proposal) {
    final ConsensusRoundIdentifier proposalRoundIdentifier = proposal.getRoundIdentifier();

    if (proposalRoundIdentifier.getRoundNumber() == 0) {
      return validateRoundZeroProposalHasNoRoundChangeCertificate(proposal);
    } else {
      return validateRoundChangeCertificateIsValidAndMatchesProposedBlock(proposal);
    }
  }

  private boolean validateRoundZeroProposalHasNoRoundChangeCertificate(final Proposal proposal) {
    if (proposal.getRoundChangeCertificate().isPresent()) {
      LOG.info(
          "Illegal Proposal message, round-0 proposal must not contain a round change certificate.");
      return false;
    }
    return true;
  }

  private boolean validateRoundChangeCertificateIsValidAndMatchesProposedBlock(
      final Proposal proposal) {

    final Optional<RoundChangeCertificate> roundChangeCertificate =
        proposal.getRoundChangeCertificate();

    if (!roundChangeCertificate.isPresent()) {
      LOG.info(
          "Illegal Proposal message, rounds other than 0 must contain a round change certificate.");
      return false;
    }

    final RoundChangeCertificate roundChangeCert = roundChangeCertificate.get();

    if (!roundChangeCertificateValidator.validateRoundChangeMessagesAndEnsureTargetRoundMatchesRoot(
        proposal.getRoundIdentifier(), roundChangeCert)) {
      LOG.info("Illegal Proposal message, embedded RoundChangeCertificate is not self-consistent");
      return false;
    }

    if (!roundChangeCertificateValidator.validateProposalMessageMatchesLatestPrepareCertificate(
        roundChangeCert, proposal.getBlock())) {
      LOG.info(
          "Illegal Proposal message, piggybacked block does not match latest PrepareCertificate");
      return false;
    }

    return true;
  }

  /**
   * Validate prepare.
   *
   * @param msg the msg
   * @return the boolean
   */
  public boolean validatePrepare(final Prepare msg) {
    return signedDataValidator.validatePrepare(msg.getSignedPayload());
  }

  /**
   * Validate commit.
   *
   * @param msg the msg
   * @return the boolean
   */
  public boolean validateCommit(final Commit msg) {
    return signedDataValidator.validateCommit(msg.getSignedPayload());
  }
}
