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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;

public interface BaseFeeMarket extends FeeMarket {

  enum ValidationMode {
    NONE,
    INITIAL,
    ONGOING
  }

  @Override
  default boolean implementsBaseFee() {
    return true;
  }

  long getBasefeeMaxChangeDenominator();

  Wei getInitialBasefee();

  long getSlackCoefficient();

  /**
   * Compute the "gas target" for this block, which results in no baseFee adjustment in a subsequent
   * block.
   *
   * @param blockHeader block header to compute gas target of.
   * @return long target gas value.
   */
  default long targetGasUsed(final BlockHeader blockHeader) {
    return blockHeader.getGasLimit() / getSlackCoefficient();
  }

  /**
   * Compute the base fee of a block from its parent.
   *
   * @param blockNumber the number of the block to compute the base fee of.
   * @param parentBlockHeader the header of the parent block.
   * @return the base fee of the block.
   */
  Wei computeBaseFee(final long blockNumber, final BlockHeader parentBlockHeader);

  ValidationMode baseFeeValidationMode(final long blockNumber);

  ValidationMode gasLimitValidationMode(final long blockNumber);

  boolean isBeforeForkBlock(final long blockNumber);
}
