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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.List;
import java.util.OptionalLong;

/**
 * Strategy interface for validating that a block's gasUsed header field is correct. Different hard
 * forks use different validation approaches based on how block gas accounting works.
 *
 * <p>Prior to EIP-7778: Block gas equals the last receipt's cumulativeGasUsed (post-refund).
 *
 * <p>EIP-7778 (Amsterdam+): Block gas is pre-refund and may differ from receipt's
 * cumulativeGasUsed. Validating a block with transactions requires the cumulativeBlockGasUsed from
 * block processing.
 */
@FunctionalInterface
public interface BlockGasUsedValidator {

  /**
   * Validates that the block's gasUsed header field is correct.
   *
   * @param header the block header containing the gasUsed to validate
   * @param receipts the transaction receipts from the block
   * @param cumulativeBlockGasUsed the cumulative block gas from processing (if available). For
   *     EIP-7778, this is the pre-refund gas. Empty when doing light validation during sync.
   * @return true if the gasUsed is valid, false otherwise
   */
  boolean validate(
      BlockHeader header, List<TransactionReceipt> receipts, OptionalLong cumulativeBlockGasUsed);

  /**
   * Pre-EIP-7778 (Frontier through Pectra): Validates that header.gasUsed equals the last receipt's
   * cumulativeGasUsed. Both values represent post-refund gas, so they should always match.
   */
  BlockGasUsedValidator FRONTIER =
      (header, receipts, blockGas) -> {
        long receiptGasUsed =
            receipts.isEmpty() ? 0 : receipts.get(receipts.size() - 1).getCumulativeGasUsed();
        return header.getGasUsed() == receiptGasUsed;
      };

  /**
   * Amsterdam (EIP-7778 + EIP-8037): Validates header.gasUsed against cumulativeBlockGasUsed when
   * available. Header.gasUsed is the larger of the block's pre-refund execution gas and its state
   * gas, while receipt.cumulativeGasUsed is the post-refund sum of both, so the receipts do not
   * determine it.
   *
   * <p>During light validation (sync), cumulativeBlockGasUsed is empty and only a block without
   * transactions is checked, which must have a gasUsed of 0. The gasUsed of any other block is not
   * validated, so the caller must have authenticated the header.
   */
  BlockGasUsedValidator AMSTERDAM =
      (header, receipts, blockGas) -> {
        if (blockGas.isPresent()) {
          return header.getGasUsed() == blockGas.getAsLong();
        }
        return !receipts.isEmpty() || header.getGasUsed() == 0;
      };
}
