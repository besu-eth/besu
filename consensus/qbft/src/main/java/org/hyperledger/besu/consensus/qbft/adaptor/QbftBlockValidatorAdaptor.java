/*
 * Copyright contributors to Besu.
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
package org.hyperledger.besu.consensus.qbft.adaptor;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.Optional;

/** Adaptor class to allow a {@link BlockValidator} to be used as a {@link QbftBlockValidator}. */
public class QbftBlockValidatorAdaptor implements QbftBlockValidator {

  private final BlockValidator blockValidator;
  private final ProtocolContext protocolContext;

  /**
   * Constructs a new Qbft block validator
   *
   * @param blockValidator The Besu block validator
   * @param protocolContext The protocol context
   */
  public QbftBlockValidatorAdaptor(
      final BlockValidator blockValidator, final ProtocolContext protocolContext) {
    this.blockValidator = blockValidator;
    this.protocolContext = protocolContext;
  }

  @Override
  public ValidationResult validateBlock(
      final QbftBlock block, final Optional<BlockAccessList> blockAccessList) {
    final BlockProcessingResult blockProcessingResult =
        blockValidator.validateAndProcessBlock(
            protocolContext,
            AdaptorUtil.toBesuBlock(block),
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            false);
    return new ValidationResult(
        blockProcessingResult.isSuccessful(), blockProcessingResult.errorMessage);
  }
}
