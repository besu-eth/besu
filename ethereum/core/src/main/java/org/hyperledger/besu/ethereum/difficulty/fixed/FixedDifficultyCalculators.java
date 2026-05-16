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
package org.hyperledger.besu.ethereum.difficulty.fixed;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.mainnet.DifficultyCalculator;

import java.math.BigInteger;

/**
 * This provides a difficulty calculator that can be used during development efforts; given
 * development (typically) uses CPU based mining, a negligible difficulty ensures tests etc. execute
 * quickly.
 */
public class FixedDifficultyCalculators {

  public static final int DEFAULT_DIFFICULTY = 100;

  public static boolean isFixedDifficultyInConfig(final GenesisConfigOptions config) {
    return config.getFixedDifficultyConfigOptions().getFixedDifficulty().isPresent();
  }

  public static DifficultyCalculator calculator(final GenesisConfigOptions config) {
    long difficulty = config.getFixedDifficultyConfigOptions().getFixedDifficulty().getAsLong();
    return (time, parent) -> BigInteger.valueOf(difficulty);
  }
}
