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
package org.hyperledger.besu.consensus.merge;

import static org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderValidator.MIN_GAS_LIMIT;
import static org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderValidator.TIMESTAMP_TOLERANCE_S;

import org.hyperledger.besu.consensus.merge.headervalidationrules.ConstantOmmersHashRule;
import org.hyperledger.besu.consensus.merge.headervalidationrules.IncrementalTimestampRule;
import org.hyperledger.besu.consensus.merge.headervalidationrules.NoDifficultyRule;
import org.hyperledger.besu.consensus.merge.headervalidationrules.NoNonceRule;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.BlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.MainnetBlockHeaderValidator;
import org.hyperledger.besu.ethereum.mainnet.feemarket.BaseFeeMarket;
import org.hyperledger.besu.ethereum.mainnet.feemarket.FeeMarket;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.AncestryValidationRule;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.BaseFeeMarketBlockHeaderGasPriceValidationRule;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.ExtraDataMaxLengthValidationRule;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.GasLimitRangeAndDeltaValidationRule;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.GasUsageValidationRule;
import org.hyperledger.besu.ethereum.mainnet.headervalidationrules.TimestampBoundedByFutureParameter;

import java.util.Optional;

/** The Merge validation ruleset factory. */
public class MergeValidationRulesetFactory {

  /** Default constructor. */
  private MergeValidationRulesetFactory() {}

  /**
   * Creates a set of rules which when executed will determine if a given block header is valid with
   * respect to its parent (or chain).
   *
   * <p>Specifically the set of rules provided by this function are to be used for a Mainnet Merge
   * chain.
   *
   * @param feeMarket the applicable {@link FeeMarket}
   * @return the header validator.
   */
  public static BlockHeaderValidator.Builder mergeBlockHeaderValidator(final FeeMarket feeMarket) {
    if (!feeMarket.implementsBaseFee()) {
      return MainnetBlockHeaderValidator.create();
    }

    var baseFeeMarket = (BaseFeeMarket) feeMarket;

    return new BlockHeaderValidator.Builder()
        .addRule(new AncestryValidationRule())
        .addRule(new GasUsageValidationRule())
        .addRule(
            new GasLimitRangeAndDeltaValidationRule(
                MIN_GAS_LIMIT, Long.MAX_VALUE, Optional.of(baseFeeMarket)))
        .addRule(new TimestampBoundedByFutureParameter(TIMESTAMP_TOLERANCE_S))
        .addRule(new ExtraDataMaxLengthValidationRule(BlockHeader.MAX_EXTRA_DATA_BYTES))
        .addRule((new BaseFeeMarketBlockHeaderGasPriceValidationRule(baseFeeMarket)))
        .addRule(new ConstantOmmersHashRule())
        .addRule(new NoNonceRule())
        .addRule(new NoDifficultyRule())
        .addRule(new IncrementalTimestampRule());
  }
}
