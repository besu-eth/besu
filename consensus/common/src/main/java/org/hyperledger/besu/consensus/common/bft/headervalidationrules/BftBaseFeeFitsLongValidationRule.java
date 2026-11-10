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
package org.hyperledger.besu.consensus.common.bft.headervalidationrules;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.DetachedBlockHeaderValidationRule;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rejects a BFT block header whose base fee does not fit in a long.
 *
 * <p>BFT header validation does not enforce the EIP-1559 base-fee transition, so without this bound
 * a proposer can admit a header carrying a base fee above {@code 2^63 - 1}. Downstream fee-market
 * and block-creation code converts the base fee with {@code toLong()}; once such a header is on the
 * chain head, that conversion fails for every node and stalls block production. A base fee that
 * large is invalid under any fee market, so this check is independent of the configured fee market.
 */
public class BftBaseFeeFitsLongValidationRule implements DetachedBlockHeaderValidationRule {

  /** Default constructor. */
  public BftBaseFeeFitsLongValidationRule() {}

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BftBaseFeeFitsLongValidationRule.class);

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    final Optional<Wei> baseFee = header.getBaseFee();
    if (baseFee.isPresent() && !baseFee.get().fitsLong()) {
      LOGGER.info("Invalid block header: base fee {} is too large to fit in a long", baseFee.get());
      return false;
    }
    return true;
  }
}
