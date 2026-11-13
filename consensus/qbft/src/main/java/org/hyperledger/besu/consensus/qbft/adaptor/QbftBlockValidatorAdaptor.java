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

import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.BftHelpers;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlock;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.BlockProcessingResult;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.HeaderValidationMode;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.ethereum.mainnet.inclusionlist.InclusionListTransactions;

import java.util.Optional;

/** Adaptor class to allow a {@link BlockValidator} to be used as a {@link QbftBlockValidator}. */
public class QbftBlockValidatorAdaptor implements QbftBlockValidator {

  private final BlockValidator blockValidator;
  private final ProtocolContext protocolContext;
  private final BftExtraDataCodec bftExtraDataCodec;
  private final ValidatedBlockCache validatedBlockCache;

  /**
   * Constructs a new Qbft block validator
   *
   * @param blockValidator The Besu block validator
   * @param protocolContext The protocol context
   * @param bftExtraDataCodec the codec to compute the on-chain hash of the block
   * @param validatedBlockCache outputs of created and validated blocks
   */
  public QbftBlockValidatorAdaptor(
      final BlockValidator blockValidator,
      final ProtocolContext protocolContext,
      final BftExtraDataCodec bftExtraDataCodec,
      final ValidatedBlockCache validatedBlockCache) {
    this.blockValidator = blockValidator;
    this.protocolContext = protocolContext;
    this.bftExtraDataCodec = bftExtraDataCodec;
    this.validatedBlockCache = validatedBlockCache;
  }

  @Override
  public ValidationResult validateBlock(
      final QbftBlock block, final Optional<BlockAccessList> blockAccessList) {
    final Block proposedBlock = AdaptorUtil.toBesuBlock(block);
    // hashed as on-chain: cache, trie log and bad block keys match the committed block, any round
    final Block besuBlock =
        new Block(
            BftHelpers.toOnchainHeader(bftExtraDataCodec, proposedBlock.getHeader()),
            proposedBlock.getBody());
    final Hash blockHash = besuBlock.getHash();
    if (isExecuted(besuBlock, blockAccessList)) {
      return new ValidationResult(true, Optional.empty());
    }
    if (!validatedBlockCache.tryStartExecution(blockHash)) {
      // e.g. NewBlock received before the proposal: the network is importing this block, wait for
      // the import instead of executing the block a second time
      validatedBlockCache.awaitExecution(blockHash);
      return isExecuted(besuBlock, blockAccessList)
          ? new ValidationResult(true, Optional.empty())
          : execute(besuBlock, blockAccessList);
    }
    try {
      return execute(besuBlock, blockAccessList);
    } finally {
      validatedBlockCache.endExecution(blockHash);
    }
  }

  // created or validated before with the same body and BAL, or imported from the network, which
  // validated it fully
  private boolean isExecuted(final Block block, final Optional<BlockAccessList> blockAccessList) {
    return validatedBlockCache
            .get(block.getHash())
            .filter(
                validatedBlock ->
                    validatedBlock.isExecutedBody(block.getBody())
                        && validatedBlock.blockAccessList().equals(blockAccessList))
            .isPresent()
        || protocolContext
            .getBlockchain()
            .getBlockBody(block.getHash())
            .filter(importedBody -> ValidatedBlockCache.isSameBody(importedBody, block.getBody()))
            .isPresent();
  }

  private ValidationResult execute(
      final Block block, final Optional<BlockAccessList> blockAccessList) {
    final BlockProcessingResult blockProcessingResult =
        blockValidator.validateAndProcessBlock(
            protocolContext,
            block,
            HeaderValidationMode.LIGHT,
            HeaderValidationMode.FULL,
            blockAccessList,
            InclusionListTransactions.EMPTY,
            false);
    if (blockProcessingResult.isSuccessful()) {
      blockProcessingResult
          .getYield()
          .ifPresent(
              outputs ->
                  validatedBlockCache.put(
                      block.getHash(),
                      new ValidatedBlockCache.ValidatedBlock(
                          block.getHeader().getNumber(),
                          block.getBody(),
                          outputs.getReceipts(),
                          outputs.getBlockAccessList())));
    }
    return new ValidationResult(
        blockProcessingResult.isSuccessful(), blockProcessingResult.errorMessage);
  }
}
