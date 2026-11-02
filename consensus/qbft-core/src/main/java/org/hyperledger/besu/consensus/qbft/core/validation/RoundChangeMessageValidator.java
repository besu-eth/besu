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
import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.messagewrappers.RoundChange;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparePayload;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Round change message validator. */
public class RoundChangeMessageValidator {

  private static final String ERROR_PREFIX = "Invalid RoundChange Message";

  private static final Logger LOG = LoggerFactory.getLogger(RoundChangeMessageValidator.class);

  private final RoundChangePayloadValidator roundChangePayloadValidator;
  private final long quorumMessageCount;
  private final long chainHeight;
  private final Collection<Address> validators;
  private final QbftProtocolSchedule protocolSchedule;

  /**
   * Instantiates a new Round change message validator.
   *
   * @param roundChangePayloadValidator the round change payload validator
   * @param quorumMessageCount the quorum message count
   * @param chainHeight the chain height
   * @param validators the validators
   * @param protocolSchedule the protocol context
   */
  public RoundChangeMessageValidator(
      final RoundChangePayloadValidator roundChangePayloadValidator,
      final long quorumMessageCount,
      final long chainHeight,
      final Collection<Address> validators,
      final QbftProtocolSchedule protocolSchedule) {
    this.roundChangePayloadValidator = roundChangePayloadValidator;
    this.quorumMessageCount = quorumMessageCount;
    this.chainHeight = chainHeight;
    this.validators = validators;
    this.protocolSchedule = protocolSchedule;
  }

  /**
   * Validate.
   *
   * @param msg the Round Change msg
   * @return the boolean
   */
  public boolean validate(final RoundChange msg) {

    if (!roundChangePayloadValidator.validate(msg.getSignedPayload())) {
      LOG.info("{}: embedded payload was invalid", ERROR_PREFIX);
      return false;
    }

    if (msg.getProposedBlock().isPresent()) {
      return validateWithBlock(msg);
    }

    return msg.getPreparedRoundMetadata().isEmpty();
  }

  private boolean validateBlock(
      final QbftBlock block, final Optional<BlockAccessList> blockAccessList) {

    final QbftBlockValidator blockValidator = protocolSchedule.getBlockValidator(block.getHeader());

    final var validationResult = blockValidator.validateBlock(block, blockAccessList);

    if (!validationResult.success()) {
      LOG.info(
          "{}: block did not pass validation. Reason {}",
          ERROR_PREFIX,
          validationResult.errorMessage());
      return false;
    }

    return true;
  }

  private boolean validateWithBlock(final RoundChange msg) {
    final QbftBlock block = msg.getProposedBlock().get();

    // The metadata checks below are decidable from data already in hand and cost nothing, so they
    // run before validateBlock, which executes every transaction in the block. The proposed block
    // rides outside the payload signature, so without this ordering anyone who captures a single
    // genuine validator-signed RoundChange can replay it with arbitrary blocks substituted, buying
    // a full block execution on the BFT consensus thread per replay - no validator key required.
    // MessageTracker de-duplicates on raw message bytes, so every substituted block is a fresh
    // message and de-duplication does not bound this.
    if (msg.getPreparedRoundMetadata().isEmpty()) {
      LOG.info("{}: Prepared block specified, but prepared metadata absent", ERROR_PREFIX);
      return false;
    }

    final PreparedRoundMetadata metadata = msg.getPreparedRoundMetadata().get();

    if (!metadata.getPreparedBlockHash().equals(block.getHash())) {
      LOG.info("{}: Prepared metadata hash does not match supplied block", ERROR_PREFIX);
      return false;
    }

    if (!validateBlock(block, msg.getBlockAccessList())) {
      return false;
    }

    return validatePrepares(metadata, msg.getPrepares());
  }

  private boolean validatePrepares(
      final PreparedRoundMetadata metaData, final List<SignedData<PreparePayload>> prepares) {

    final ConsensusRoundIdentifier preparedRoundIdentifier =
        new ConsensusRoundIdentifier(chainHeight, metaData.getPreparedRound());

    final PrepareValidator validator =
        new PrepareValidator(validators, preparedRoundIdentifier, metaData.getPreparedBlockHash());

    if (hasDuplicateAuthors(prepares)) {
      LOG.info("{}: multiple prepares from the same author.", ERROR_PREFIX);
      return false;
    }

    if (!hasSufficientEntries(prepares, quorumMessageCount)) {
      LOG.info("{}: insufficient Prepare messages piggybacked.", ERROR_PREFIX);
      return false;
    }

    return prepares.stream().allMatch(validator::validate);
  }
}
