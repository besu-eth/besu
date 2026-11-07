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
package org.hyperledger.besu.consensus.qbft.core.types;

import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

import java.util.Optional;

/** Imports a block into the chain. */
public interface QbftBlockImporter {

  /**
   * Import a block into the chain.
   *
   * @param block to import
   * @param blockAccessList block access list
   * @return true if the block was successfully imported, false otherwise
   */
  boolean importBlock(QbftBlock block, Optional<BlockAccessList> blockAccessList);

  /**
   * Claim a proposal: the current round is validating it, or has accepted it and will import it if
   * the round commits. Block propagation does not import a claimed block, so that it is not
   * executed a second time while the round imports it.
   *
   * @param proposal the proposed block
   */
  default void claim(final QbftBlock proposal) {}

  /**
   * Release a claim made by {@link #claim}: the proposal was rejected, its round ended, or its
   * import was attempted.
   *
   * @param proposal the proposed block
   */
  default void releaseClaim(final QbftBlock proposal) {}
}
