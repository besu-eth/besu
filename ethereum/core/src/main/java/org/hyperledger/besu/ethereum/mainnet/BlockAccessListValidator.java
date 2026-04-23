/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

/** Validates block access lists according to protocol rules. */
public interface BlockAccessListValidator {

  /**
   * Rejects any block that includes a BAL (returns false when present). Used for forks before
   * Amsterdam, where blocks must not contain a block access list.
   */
  BlockAccessListValidator ALWAYS_REJECT_BAL =
      (blockAccessList, header, nbTransactions) ->
          blockAccessList.isEmpty() && header.getBalHash().isEmpty();

  /**
   * Full validation of an <em>imported</em> block access list (e.g. from a peer): hash, EIP-7928
   * item size, canonical ordering, and index constraints per EIP-7928.
   *
   * @param blockAccessList the optional block access list to validate (empty if block has no BAL)
   * @param blockHeader the block header containing gas limit and other context
   * @param nbTransactions number of transactions in the block (must be &ge; 0)
   * @return true if the block access list is valid or absent, false otherwise
   */
  boolean validate(
      Optional<BlockAccessList> blockAccessList, BlockHeader blockHeader, int nbTransactions);

  /**
   * During block execution: EIP-7928 item-size budget only (running count of addresses plus storage
   * keys vs block gas limit). Call after each merged transaction view for fail-fast. After
   * post-execution updates, size is checked again together with the header hash in {@link
   * #validateExecutedBlockAccessListAfterBuild}.
   *
   * @param itemCount current BAL item count while merging partial views
   * @param blockHeader block or pending header (gas limit); during mining this is a {@link
   *     ProcessableBlockHeader}
   */
  default BlockAccessListItemSizeCheck validateExecutedBlockAccessListItemSize(
      final long itemCount, final ProcessableBlockHeader blockHeader) {
    return BlockAccessListItemSizeCheck.withinBudget();
  }

  /**
   * Same as {@link #validateExecutedBlockAccessListItemSize(long, ProcessableBlockHeader)} but uses
   * {@code protocolSpecForItemCost} for {@link
   * org.hyperledger.besu.evm.gascalculator.GasCalculator#getBlockAccessListItemCost()} so block
   * building can align with the mining {@link ProtocolSpec} instead of only the validator's
   * schedule.
   *
   * @param protocolSpecForItemCost spec whose gas calculator defines the EIP-7928 item cost
   */
  default BlockAccessListItemSizeCheck validateExecutedBlockAccessListItemSize(
      final long itemCount,
      final ProcessableBlockHeader blockHeader,
      final ProtocolSpec protocolSpecForItemCost) {
    return validateExecutedBlockAccessListItemSize(itemCount, blockHeader);
  }

  /**
   * After the complete executed BAL is built (post-withdrawals/requests, etc.): when the header
   * omits {@code balHash}, returns empty. Otherwise EIP-7928 item size on the built list, then
   * verify {@code balHash} matches.
   *
   * @param executedBal BAL built from execution traces
   * @param blockHeader block header (gas limit, optional {@code balHash})
   * @param suppliedBlockAccessList BAL supplied with the block payload (e.g. engine), for mismatch
   *     diagnostics only
   * @param logBalDetailsOnHashMismatch when true, log executed and supplied BAL bodies on hash
   *     mismatch
   * @return empty if valid; otherwise a {@link BlockAccessListValidationError}
   */
  default Optional<BlockAccessListValidationError> validateExecutedBlockAccessListAfterBuild(
      final BlockAccessList executedBal,
      final BlockHeader blockHeader,
      final Optional<BlockAccessList> suppliedBlockAccessList,
      final boolean logBalDetailsOnHashMismatch) {
    return Optional.empty();
  }
}
