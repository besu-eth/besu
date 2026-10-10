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
package org.hyperledger.besu.util;

import static org.hyperledger.besu.config.NetworkDefinition.EPHEMERY;

import org.hyperledger.besu.config.GenesisConfig;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

/**
 * The Generate Ephemery Genesis Updater. Checks for update based on the set period and update the
 * Ephemery genesis in memory
 */
public class EphemeryGenesisUpdater {
  private static final int PERIOD_IN_DAYS = 28;
  private static final long PERIOD_IN_SECONDS = (PERIOD_IN_DAYS * 24 * 60 * 60);
  /**
   * Relative activation offsets for Blob Parameter Options (BPO) on the Ephemery testnet.
   *
   * <p>Ephemery resets on a 28-day cycle ({@link #PERIOD_IN_DAYS}). Hardfork activation timestamps
   * (e.g. BPO1 and BPO2, introduced in PR #9354 / Ephemery Fusaka) are defined relative to the
   * genesis timestamp of each 28-day iteration:
   * <ul>
   *   <li>BPO1 offset: 787,032 seconds (~9.1 days / 65,586 slots) after iteration genesis
   *   <li>BPO2 offset: 1,573,464 seconds (~18.2 days / 131,122 slots) after iteration genesis
   * </ul>
   * When genesis timestamp rolls over to a new iteration, these offsets ensure bpo1Time and
   * bpo2Time are dynamically recalculated to activate at the expected point within that cycle.
   */
  static final long BPO1_OFFSET_SECONDS = 787032L;
  static final long BPO2_OFFSET_SECONDS = 1573464L;

  /**
   * Constructor for EphemeryGenesisUpdater. Initializes the genesis updater for the Ephemery
   * network.
   */
  public EphemeryGenesisUpdater() {}

  /**
   * Updates the Ephemery genesis configuration based on the predefined period.
   *
   * @param overrides a map of configuration overrides
   * @return the updated GenesisConfigFile
   * @throws RuntimeException if an error occurs during the update process
   */
  public static GenesisConfig updateGenesis(final Map<String, String> overrides)
      throws RuntimeException {
    GenesisConfig genesisConfig;
    try {
      if (EPHEMERY.getGenesisFile() == null) {
        throw new IOException("Genesis file or config options are null");
      }
      genesisConfig = GenesisConfig.fromResource(EPHEMERY.getGenesisFile());
      long genesisTimestamp = genesisConfig.getTimestamp();
      Optional<BigInteger> genesisChainId = genesisConfig.getConfigOptions().getChainId();
      long currentTimestamp = Instant.now().getEpochSecond();
      long periodsSinceGenesis =
          ChronoUnit.DAYS.between(Instant.ofEpochSecond(genesisTimestamp), Instant.now())
              / PERIOD_IN_DAYS;

      long updatedTimestamp = genesisTimestamp + (periodsSinceGenesis * PERIOD_IN_SECONDS);
      BigInteger updatedChainId =
          genesisChainId
              .orElseThrow(() -> new IllegalStateException("ChainId not present"))
              .add(BigInteger.valueOf(periodsSinceGenesis));
      // has a period elapsed since original ephemery genesis time? if so, override chainId and
      // timestamp
      if (currentTimestamp > (genesisTimestamp + PERIOD_IN_SECONDS)) {
        overrides.put("chainId", String.valueOf(updatedChainId));
        overrides.put("timestamp", String.valueOf(updatedTimestamp));
        overrides.put("bpo1Time", String.valueOf(updatedTimestamp + BPO1_OFFSET_SECONDS));
        overrides.put("bpo2Time", String.valueOf(updatedTimestamp + BPO2_OFFSET_SECONDS));
      }
      return genesisConfig.withOverrides(overrides);
    } catch (IOException e) {
      throw new RuntimeException("Error updating ephemery genesis: " + e.getMessage(), e);
    }
  }
}
