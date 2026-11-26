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

import org.hyperledger.besu.consensus.common.bft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.common.bft.payload.Payload;
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.ibft.payload.CommitPayload;
import org.hyperledger.besu.consensus.ibft.payload.PreparePayload;
import org.hyperledger.besu.consensus.ibft.payload.ProposalPayload;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Util;

import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Signed data validator. */
public class SignedDataValidator {

  private static final Logger LOG = LoggerFactory.getLogger(SignedDataValidator.class);

  private final Collection<Address> validators;
  private final Address expectedProposer;
  private final ConsensusRoundIdentifier roundIdentifier;

  private Optional<SignedData<ProposalPayload>> proposal = Optional.empty();

  /**
   * Instantiates a new Signed data validator.
   *
   * @param validators the validators
   * @param expectedProposer the expected proposer
   * @param roundIdentifier the round identifier
   */
  public SignedDataValidator(
      final Collection<Address> validators,
      final Address expectedProposer,
      final ConsensusRoundIdentifier roundIdentifier) {
    this.validators = validators;
    this.expectedProposer = expectedProposer;
    this.roundIdentifier = roundIdentifier;
  }

  /**
   * Validate proposal.
   *
   * @param msg the msg
   * @return the boolean
   */
  public boolean validateProposal(final SignedData<ProposalPayload> msg) {

    if (proposal.isPresent()) {
      return handleSubsequentProposal(proposal.get(), msg);
    }

    if (!validateProposalSignedDataPayload(msg)) {
      return false;
    }

    proposal = Optional.of(msg);
    return true;
  }

  private boolean validateProposalSignedDataPayload(final SignedData<ProposalPayload> msg) {

    if (!msg.getPayload().getRoundIdentifier().equals(roundIdentifier)) {
      LOG.info("Invalid Proposal message, does not match current round.");
      return false;
    }

    if (!msg.getAuthor().equals(expectedProposer)) {
      LOG.info(
          "Invalid Proposal message, was not created by the proposer expected for the "
              + "associated round.");
      return false;
    }

    return true;
  }

  private boolean handleSubsequentProposal(
      final SignedData<ProposalPayload> existingMsg, final SignedData<ProposalPayload> newMsg) {
    if (!existingMsg.getAuthor().equals(newMsg.getAuthor())) {
      LOG.info("Received subsequent invalid Proposal message; sender differs from original.");
      return false;
    }

    final ProposalPayload existingData = existingMsg.getPayload();
    final ProposalPayload newData = newMsg.getPayload();

    if (!proposalMessagesAreIdentical(existingData, newData)) {
      LOG.info("Received subsequent invalid Proposal message; content differs from original.");
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
  public boolean validatePrepare(final SignedData<PreparePayload> msg) {
    final String msgType = "Prepare";

    if (!isMessageForCurrentRoundFromValidatorAndProposalAvailable(msg, msgType)) {
      return false;
    }

    if (msg.getAuthor().equals(expectedProposer)) {
      LOG.info("Illegal Prepare message; was sent by the round's proposer.");
      return false;
    }

    return validateDigestMatchesProposal(msg.getPayload().getDigest(), msgType);
  }

  /**
   * Validate commit.
   *
   * @param msg the msg
   * @return the boolean
   */
  public boolean validateCommit(final SignedData<CommitPayload> msg) {
    final String msgType = "Commit";

    if (!isMessageForCurrentRoundFromValidatorAndProposalAvailable(msg, msgType)) {
      return false;
    }

    final Hash proposedBlockDigest = proposal.get().getPayload().getDigest();
    final Address commitSealCreator =
        Util.signatureToAddress(msg.getPayload().getCommitSeal(), proposedBlockDigest);

    // A crafted Commit can carry a seal that does not recover to an address, for which
    // Util.signatureToAddress returns null. Reject it rather than dereference null below, which
    // would throw and lose the round for every validator processing the message.
    if (commitSealCreator == null) {
      LOG.info("Invalid Commit message. Seal did not recover to an address.");
      return false;
    }

    if (!commitSealCreator.equals(msg.getAuthor())) {
      LOG.info("Invalid Commit message. Seal was not created by the message transmitter.");
      return false;
    }

    return validateDigestMatchesProposal(msg.getPayload().getDigest(), msgType);
  }

  private boolean isMessageForCurrentRoundFromValidatorAndProposalAvailable(
      final SignedData<? extends Payload> msg, final String msgType) {

    if (!msg.getPayload().getRoundIdentifier().equals(roundIdentifier)) {
      LOG.info("Invalid {} message, does not match current round.", msgType);
      return false;
    }

    if (!validators.contains(msg.getAuthor())) {
      LOG.info(
          "Invalid {} message, was not transmitted by a validator for the " + "associated round.",
          msgType);
      return false;
    }

    if (!proposal.isPresent()) {
      LOG.info(
          "Unable to validate {} message. No Proposal exists against which to validate "
              + "block digest.",
          msgType);
      return false;
    }
    return true;
  }

  private boolean validateDigestMatchesProposal(final Hash digest, final String msgType) {
    final Hash proposedBlockDigest = proposal.get().getPayload().getDigest();
    if (!digest.equals(proposedBlockDigest)) {
      LOG.info(
          "Illegal {} message, digest does not match the digest in the Prepare Message.", msgType);
      return false;
    }
    return true;
  }

  private boolean proposalMessagesAreIdentical(
      final ProposalPayload right, final ProposalPayload left) {
    return right.getDigest().equals(left.getDigest())
        && right.getRoundIdentifier().equals(left.getRoundIdentifier());
  }
}
