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
package org.hyperledger.besu.consensus.qbft.headervalidationrules;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.AbstractGasLimitSpecification;
import org.hyperledger.besu.ethereum.mainnet.DetachedBlockHeaderValidationRule;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures the gasLimit specified in the supplied block header is within the bounds specified at
 * construction, but, unlike {@link
 * org.hyperledger.besu.ethereum.mainnet.headervalidationrules.GasLimitRangeAndDeltaValidationRule},
 * does <b>not</b> validate the delta of the gasLimit relative to its parent.
 *
 * <p>This is used for GoQuorum compatibility mode (issue #10723): GoQuorum's QBFT allows block
 * proposers to change the gas limit by an arbitrary amount per block, so the per-block delta bound
 * cannot be enforced, while the basic min/max range check is still applied.
 */
public class GoQuorumGasLimitRangeValidationRule extends AbstractGasLimitSpecification
    implements DetachedBlockHeaderValidationRule {

  private static final Logger LOG =
      LoggerFactory.getLogger(GoQuorumGasLimitRangeValidationRule.class);

  private final Optional<BaseFeeMarket> baseFeeMarket;

  /**
   * Instantiates a new GoQuorum gas limit range validation rule.
   *
   * @param minGasLimit the minimum gas limit
   * @param maxGasLimit the maximum gas limit
   * @param baseFeeMarket an {@link Optional} wrapping {@link BaseFeeMarket} class if appropriate
   */
  public GoQuorumGasLimitRangeValidationRule(
      final long minGasLimit, final long maxGasLimit, final Optional<BaseFeeMarket> baseFeeMarket) {
    super(minGasLimit, maxGasLimit);
    this.baseFeeMarket = baseFeeMarket;
  }

  /**
   * Instantiates a new GoQuorum gas limit range validation rule with no base fee market.
   *
   * @param minGasLimit the minimum gas limit
   * @param maxGasLimit the maximum gas limit
   */
  public GoQuorumGasLimitRangeValidationRule(final long minGasLimit, final long maxGasLimit) {
    this(minGasLimit, maxGasLimit, Optional.empty());
  }

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    final long gasLimit = header.getGasLimit();

    if ((gasLimit < minGasLimit) || (gasLimit > maxGasLimit)) {
      LOG.info(
          "Invalid block header: gasLimit = {} is outside range {} --> {}",
          gasLimit,
          minGasLimit,
          maxGasLimit);
      return false;
    }

    if (baseFeeMarket.isEmpty() && header.getBaseFee().isPresent()) {
      LOG.info(
          "Invalid block header: basefee should not be present in a block without a base fee market");
      return false;
    }

    // The gas limit delta relative to the parent is intentionally not validated here: GoQuorum's
    // QBFT permits proposers to change the gas limit by an arbitrary amount per block (issue
    // #10723).
    return true;
  }

  @Override
  public String toString() {
    return "GoQuorumGasLimitRangeValidation";
  }
}
