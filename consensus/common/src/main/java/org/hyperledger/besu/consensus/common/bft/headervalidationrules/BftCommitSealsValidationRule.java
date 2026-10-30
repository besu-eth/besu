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
package org.hyperledger.besu.consensus.common.bft.headervalidationrules;

import static org.hyperledger.besu.consensus.common.bft.BftHelpers.calculateRequiredValidatorQuorum;

import org.hyperledger.besu.consensus.common.bft.BftContext;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.AttachedBlockHeaderValidationRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures the commit seals in the block header were created by known validators (as determined by
 * tracking votes and validator state on the blockchain).
 *
 * <p>This also ensures sufficient commit seals exist in the block to make it valid.
 */
public class BftCommitSealsValidationRule implements AttachedBlockHeaderValidationRule {

  private static final Logger LOGGER = LoggerFactory.getLogger(BftCommitSealsValidationRule.class);

  /** Default constructor. */
  public BftCommitSealsValidationRule() {}

  @Override
  public boolean validate(
      final BlockHeader header, final BlockHeader parent, final ProtocolContext protocolContext) {
    final BftContext bftContext = protocolContext.getConsensusContext(BftContext.class);
    final Collection<Address> storedValidators =
        bftContext.getValidatorProvider().getValidatorsAfterBlock(parent);

    // Bound the seal count before recovering any key. Each recovery is an ecrecover, the seals
    // list is decoded with an unbounded readList, and BFT headers carry no extraData length rule
    // - so without this check any connected eth peer can put an arbitrary number of seals in a
    // NEW_BLOCK announcement and have every one of them recovered during FULL header validation
    // before the header is rejected. Measured: 20,000 byte-identical seals cost ~1s of recovery
    // to yield a single distinct committer.
    //
    // A valid BFT header carries at most one seal per validator, so anything above that is
    // rejectable on an integer comparison. Capping here also bounds the work the checks below can
    // be made to do, which is why the duplicate and quorum checks that follow are safe to leave
    // operating on recovered committers.
    final int sealCount = bftContext.getBlockInterface().getExtraData(header).getSeals().size();

    if (sealCount > storedValidators.size()) {
      LOGGER.info(
          "Invalid block header: More commit seals than validators. (Validators {}, seals {})",
          storedValidators.size(),
          sealCount);
      return false;
    }

    if (sealCount < calculateRequiredValidatorQuorum(storedValidators.size())) {
      LOGGER.info(
          "Invalid block header: Insufficient committers to seal block. (Required {}, received {})",
          calculateRequiredValidatorQuorum(storedValidators.size()),
          sealCount);
      return false;
    }

    final List<Address> committers = bftContext.getBlockInterface().getCommitters(header);
    final List<Address> committersWithoutDuplicates = new ArrayList<>(new HashSet<>(committers));

    if (committers.size() != committersWithoutDuplicates.size()) {
      LOGGER.info("Invalid block header: Duplicated seals found in header.");
      return false;
    }

    return validateCommitters(committersWithoutDuplicates, storedValidators);
  }

  private boolean validateCommitters(
      final Collection<Address> committers, final Collection<Address> storedValidators) {

    final int minimumSealsRequired = calculateRequiredValidatorQuorum(storedValidators.size());
    if (committers.size() < minimumSealsRequired) {
      LOGGER.info(
          "Invalid block header: Insufficient committers to seal block. (Required {}, received {})",
          minimumSealsRequired,
          committers.size());
      return false;
    }

    if (!storedValidators.containsAll(committers)) {
      LOGGER.info(
          "Invalid block header: Not all committers are in the locally maintained validator list. validators={} committers={}",
          storedValidators,
          committers);
      return false;
    }

    return true;
  }

  @Override
  public boolean includeInLightValidation() {
    return false;
  }

  @Override
  public String toString() {
    return "BftCommitSealsValidation";
  }
}
