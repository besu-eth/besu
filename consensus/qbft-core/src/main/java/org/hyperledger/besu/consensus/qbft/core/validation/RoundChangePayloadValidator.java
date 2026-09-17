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
package org.hyperledger.besu.consensus.qbft.core.validation;

import org.hyperledger.besu.consensus.common.bft.payload.SignedData;
import org.hyperledger.besu.consensus.qbft.core.payload.PreparedRoundMetadata;
import org.hyperledger.besu.consensus.qbft.core.payload.RoundChangePayload;
import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Note: This does not validate that the received payload is for a future round, only that it was
 * signed by a known validator, and is for the current chain height. Future-round check must be
 * performed elsewhere (eg. the BlockHeightManager)
 */
public class RoundChangePayloadValidator {

  private static final String ERROR_PREFIX = "Invalid RoundChange Payload";
  private static final Logger LOG = LoggerFactory.getLogger(RoundChangePayloadValidator.class);

  /** maximum allowed round */
  public static final int MAX_ALLOWED_ROUND = 1000;

  private final Collection<Address> validators;
  private final long chainHeight;

  /**
   * Instantiates a new Round change payload validator.
   *
   * @param validators the validators
   * @param chainHeight the chain height
   */
  public RoundChangePayloadValidator(final Collection<Address> validators, final long chainHeight) {
    this.validators = validators;
    this.chainHeight = chainHeight;
  }

  /**
   * Validate.
   *
   * @param signedPayload the signed payload
   * @return the boolean
   */
  public boolean validate(final SignedData<RoundChangePayload> signedPayload) {

    if (!validators.contains(signedPayload.getAuthor())) {
      LOG.info("{}: did not originate from a recognized validator.", ERROR_PREFIX);
      return false;
    }

    final RoundChangePayload payload = signedPayload.getPayload();

    if (payload.getRoundIdentifier().getSequenceNumber() != chainHeight) {
      LOG.info("{}: did not target expected height", ERROR_PREFIX);
      return false;
    }

    final int targetRound = payload.getRoundIdentifier().getRoundNumber();
    if (targetRound <= 0 || targetRound > MAX_ALLOWED_ROUND) {
      LOG.info("{}: target round number out of range [1, {}]", ERROR_PREFIX, MAX_ALLOWED_ROUND);
      return false;
    }

    if (payload.getPreparedRoundMetadata().isPresent()) {
      final PreparedRoundMetadata metadata = payload.getPreparedRoundMetadata().get();
      if (metadata.getPreparedRound() >= targetRound) {
        LOG.info("{}: prepared metadata is from a round ahead of target round", ERROR_PREFIX);
        return false;
      }

      if (metadata.getPreparedRound() < 0) {
        LOG.info("{}: prepared metadata is from a negative round number", ERROR_PREFIX);
        return false;
      }
    }
    return true;
  }
}
