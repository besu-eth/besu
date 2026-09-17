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
package org.hyperledger.besu.ethereum;

import org.hyperledger.besu.ethereum.mainnet.BlockAccessListValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockBodyValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.BlockProcessor;

/** Utility class for creating a block validators. */
public class MainnetBlockValidatorBuilder {
  private static final int OSAKA_MAX_BLOCK_SIZE = 10_485_760; // 10 MB
  private static final int OSAKA_SAFETY_MARGIN = 2_097_152; // 2 MB
  private static final int OSAKA_MAX_RLP_BLOCK_SIZE = OSAKA_MAX_BLOCK_SIZE - OSAKA_SAFETY_MARGIN;

  /** Use the static methods to create instances of BlockValidator. */
  private MainnetBlockValidatorBuilder() {}

  /**
   * Creates a block validator for the mainnet with no block size limit.
   *
   * @param blockHeaderValidator the block header validator
   * @param blockBodyValidator the block body validator
   * @param blockProcessor the block processor
   * @param blockAccessListValidator the block access list validator
   * @return a BlockValidator instance
   */
  public static BlockValidator frontier(
      final BlockHeaderValidator blockHeaderValidator,
      final BlockBodyValidator blockBodyValidator,
      final BlockProcessor blockProcessor,
      final BlockAccessListValidator blockAccessListValidator) {
    return new MainnetBlockValidator(
        blockHeaderValidator,
        blockBodyValidator,
        blockProcessor,
        blockAccessListValidator,
        Integer.MAX_VALUE);
  }

  /**
   * Creates a block validator for the Osaka network with a specific block size limit.
   *
   * @param blockHeaderValidator the block header validator
   * @param blockBodyValidator the block body validator
   * @param blockProcessor the block processor
   * @param blockAccessListValidator the block access list validator
   * @return a BlockValidator instance with Osaka-specific settings
   */
  public static BlockValidator osaka(
      final BlockHeaderValidator blockHeaderValidator,
      final BlockBodyValidator blockBodyValidator,
      final BlockProcessor blockProcessor,
      final BlockAccessListValidator blockAccessListValidator) {
    return new MainnetBlockValidator(
        blockHeaderValidator,
        blockBodyValidator,
        blockProcessor,
        blockAccessListValidator,
        OSAKA_MAX_RLP_BLOCK_SIZE);
  }
}
