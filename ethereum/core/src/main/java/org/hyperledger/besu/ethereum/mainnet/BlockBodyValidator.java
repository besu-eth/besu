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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;

import java.util.List;
import java.util.OptionalLong;

/** Validates block bodies. */
public interface BlockBodyValidator {

  /**
   * Validates that the block body is valid.
   *
   * @param context The context to validate against
   * @param block The block to validate
   * @param receipts The receipts that correspond to the blocks transactions
   * @param worldStateRootHash The rootHash defining the world state after processing this block and
   *     all of its transactions.
   * @param ommerValidationMode The validation mode to use for ommer headers
   * @param bodyValidationMode The validation mode to use for the body
   * @return {@code true} if valid; otherwise {@code false}
   */
  default boolean validateBody(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Hash worldStateRootHash,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode) {
    return validateBody(
        context,
        block,
        receipts,
        worldStateRootHash,
        ommerValidationMode,
        bodyValidationMode,
        OptionalLong.empty());
  }

  /**
   * Validates that the block body is valid.
   *
   * @param context The context to validate against
   * @param block The block to validate
   * @param receipts The receipts that correspond to the blocks transactions
   * @param worldStateRootHash The rootHash defining the world state after processing this block and
   *     all of its transactions.
   * @param ommerValidationMode The validation mode to use for ommer headers
   * @param bodyValidationMode The validation mode to use for the body
   * @param cumulativeBlockGasUsed The cumulative block gas used from block processing (pre-refund
   *     for EIP-7778). When provided, this value is used for gas validation instead of deriving
   *     from receipts.
   * @return {@code true} if valid; otherwise {@code false}
   */
  boolean validateBody(
      final ProtocolContext context,
      final Block block,
      final List<TransactionReceipt> receipts,
      final Hash worldStateRootHash,
      final HeaderValidationMode ommerValidationMode,
      final BodyValidationMode bodyValidationMode,
      final OptionalLong cumulativeBlockGasUsed);

  /**
   * Validates that the block body is valid, but skips state root validation.
   *
   * @param context The context to validate against
   * @param block The block to validate
   * @param receipts The receipts that correspond to the blocks transactions
   * @param ommerValidationMode The validation mode to use for ommer headers
   * @param cumulativeBlockGasUsed The cumulative block gas used from block processing (pre-refund
   *     for EIP-7778). When provided, this value is used for gas validation instead of deriving
   *     from receipts. Pass {@code OptionalLong.empty()} when not available (e.g., during sync);
   *     from Amsterdam on, the gasUsed of a block with transactions is then not validated.
   * @return {@code true} if valid; otherwise {@code false}
   */
  boolean validateBodyLight(
      ProtocolContext context,
      Block block,
      List<TransactionReceipt> receipts,
      HeaderValidationMode ommerValidationMode,
      OptionalLong cumulativeBlockGasUsed);
}
