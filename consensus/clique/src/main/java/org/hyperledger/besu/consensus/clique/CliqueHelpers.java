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

import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;

import java.util.Collection;
import java.util.Comparator;

/** The Clique helpers. */
public class CliqueHelpers {
  private static CliqueContext cliqueContext;

  /** Default constructor. */
  CliqueHelpers() {}

  /**
   * Set the clique consensus context
   *
   * @param cliqueContext the clique consensus context
   */
  public static void setCliqueContext(final CliqueContext cliqueContext) {
    CliqueHelpers.cliqueContext = cliqueContext;
  }

  /**
   * Gets proposer of block.
   *
   * @param header the header
   * @return the proposer of block address
   */
  public static Address getProposerOfBlock(final BlockHeader header) {
    final CliqueExtraData extraData = CliqueExtraData.decode(header);
    return extraData.getProposerAddress();
  }

  /**
   * Gets proposer for block after.
   *
   * @param parent the parent
   * @return the proposer for block after
   */
  static Address getProposerForBlockAfter(final BlockHeader parent) {
    final CliqueProposerSelector proposerSelector =
        new CliqueProposerSelector(cliqueContext.getValidatorProvider());
    return proposerSelector.selectProposerForNextBlock(parent);
  }

  /**
   * Is signer.
   *
   * @param candidate the candidate
   * @param protocolContext the protocol context
   * @param parent the parent
   * @return the boolean
   */
  static boolean isSigner(
      final Address candidate, final ProtocolContext protocolContext, final BlockHeader parent) {
    final Collection<Address> validators =
        protocolContext
            .getConsensusContext(CliqueContext.class)
            .getValidatorProvider()
            .getValidatorsAfterBlock(parent);
    return validators.contains(candidate);
  }

  /**
   * Address is allowed to produce next block.
   *
   * @param candidate the candidate
   * @param protocolContext the protocol context
   * @param parent the parent
   * @return the boolean
   */
  public static boolean addressIsAllowedToProduceNextBlock(
      final Address candidate, final ProtocolContext protocolContext, final BlockHeader parent) {
    final ValidatorProvider validatorProvider =
        protocolContext.getConsensusContext(CliqueContext.class).getValidatorProvider();

    if (!isSigner(candidate, protocolContext, parent)) {
      return false;
    }

    final int minimumUnsignedPastBlocks =
        minimumBlocksSincePreviousSigning(parent, validatorProvider);

    final Blockchain blockchain = protocolContext.getBlockchain();
    int unsignedBlockCount = 0;
    BlockHeader localParent = parent;

    while (unsignedBlockCount < minimumUnsignedPastBlocks) {

      if (localParent.getNumber() == 0) {
        return true;
      }

      final Address parentSigner = CliqueHelpers.getProposerOfBlock(localParent);
      if (parentSigner.equals(candidate)) {
        return false;
      }
      unsignedBlockCount++;

      localParent =
          blockchain
              .getBlockHeader(localParent.getParentHash())
              .orElseThrow(() -> new IllegalStateException("The block was on a orphaned chain."));
    }

    return true;
  }

  private static int minimumBlocksSincePreviousSigning(
      final BlockHeader parent, final ValidatorProvider validatorProvider) {
    final int validatorCount = validatorProvider.getValidatorsAfterBlock(parent).size();
    // The number of contiguous blocks in which a signer may only sign 1 (as taken from clique spec)
    final int signerLimit = (validatorCount / 2) + 1;
    return signerLimit - 1;
  }

  /**
   * Install clique block choice rule.
   *
   * @param blockchain the blockchain
   * @param cliqueContext the clique context
   */
  public static void installCliqueBlockChoiceRule(
      final Blockchain blockchain, final CliqueContext cliqueContext) {
    blockchain.setBlockChoiceRule(
        // EIP-3436 - https://eips.ethereum.org/EIPS/eip-3436
        blockchain
            // 1. Choose the block with the most total difficulty.
            .getBlockChoiceRule()
            // 2. Then choose the block with the lowest block number.
            .thenComparing(Comparator.comparing(ProcessableBlockHeader::getNumber).reversed())
            // 3. Then choose the block whose validator had the least recent in-turn block
            // assignment.
            .thenComparing((BlockHeader header) -> -distanceFromInTurn(header, cliqueContext))
            // 4. Then choose the block with the lowest hash.
            .thenComparing(
                Comparator.comparing((BlockHeader h) -> h.getHash().getBytes()).reversed()));
  }

  /**
   * Distance calculation from step 3 of the forck choice rule in EIP-3436
   *
   * <p>` (header_number - validator_index) % validator_count `
   *
   * @param header The block header to calculate distance
   * @param context The clique context, holding validators last validaion
   * @return the number of blocks from when the validator in that block was most recently "in turn"
   */
  static int distanceFromInTurn(final BlockHeader header, final CliqueContext context) {
    final Address proposer = CliqueHelpers.getProposerOfBlock(header);
    final Collection<Address> validators =
        context.getValidatorProvider().getValidatorsAfterBlock(header);

    // validators is an ordered collection, but as it is a tree set the only way to get an "index"
    // is to walk the tree.
    int index = 0;
    for (final Address validator : validators) {
      if (validator.equals(proposer)) {
        break;
      }
      index++;
    }

    return (int) (header.getNumber() - index) % validators.size();
  }
}
