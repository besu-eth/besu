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
package org.hyperledger.besu.consensus.clique;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.ArrayList;
import java.util.List;

/**
 * Responsible for determining which member of the validator pool should create the next block.
 *
 * <p>It does this be determining the available validators at the previous block, then selecting the
 * appropriate validator based on the chain height.
 */
public class CliqueProposerSelector {

  private final ValidatorProvider validatorProvider;

  /**
   * Instantiates a new Clique proposer selector.
   *
   * @param validatorProvider the validator provider
   */
  public CliqueProposerSelector(final ValidatorProvider validatorProvider) {
    checkNotNull(validatorProvider);
    this.validatorProvider = validatorProvider;
  }

  /**
   * Determines which validator should create the block after that supplied.
   *
   * @param parentHeader The header of the previously received block.
   * @return The address of the node which is to propose a block for the provided Round.
   */
  public Address selectProposerForNextBlock(final BlockHeader parentHeader) {
    final List<Address> validatorSet =
        new ArrayList<>(validatorProvider.getValidatorsAfterBlock(parentHeader));

    final long nextBlockNumber = parentHeader.getNumber() + 1L;
    final int indexIntoValidators = (int) (nextBlockNumber % validatorSet.size());

    return validatorSet.get(indexIntoValidators);
  }
}
