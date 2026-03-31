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
package org.hyperledger.besu.datatypes;

import java.util.Comparator;
import java.util.stream.Stream;

/** Description and metadata for a hard fork */
public interface HardforkId {

  /**
   * The name of the hard fork.
   *
   * @return the name for the fork
   */
  String name();

  /**
   * Has the fork been finalized? i.e., could the definition change in future versions of Besu?
   *
   * @return true if the specification is finalized.
   */
  boolean finalized();

  /**
   * A brief description of the hard fork, suitable for human consumption
   *
   * @return the description of the fork.
   */
  String description();

  /** List of all Ethereum Mainnet hardforks, including future and developmental forks. */
  enum MainnetHardforkId implements HardforkId {
    /** Frontier fork. */
    FRONTIER(true, "Frontier"),
    /** Homestead fork. */
    HOMESTEAD(true, "Homestead"),
    /** DAO Recovery Init fork. */
    DAO_RECOVERY_INIT(true, "DAO Recovery Init"),
    /** DAO Recovery Transition fork. */
    DAO_RECOVERY_TRANSITION(true, "DAO Recovery Transition"),
    /** Tangerine Whistle fork. */
    TANGERINE_WHISTLE(true, "Tangerine Whistle"),
    /** Spurious Dragon fork. */
    SPURIOUS_DRAGON(true, "Spurious Dragon"),
    /** Byzantium fork. */
    BYZANTIUM(true, "Byzantium"),
    /** Constantinople fork. */
    CONSTANTINOPLE(true, "Constantinople"),
    /** Petersburg fork. */
    PETERSBURG(true, "Petersburg"),
    /** Istanbul fork. */
    ISTANBUL(true, "Istanbul"),
    /** Muir Glacier fork. */
    MUIR_GLACIER(true, "Muir Glacier"),
    /** Berlin fork. */
    BERLIN(true, "Berlin"),
    /** London fork. */
    LONDON(true, "London"),
    /** Arrow Glacier fork. */
    ARROW_GLACIER(true, "Arrow Glacier"),
    /** Gray Glacier fork. */
    GRAY_GLACIER(true, "Gray Glacier"),
    /** Paris fork. */
    PARIS(true, "Paris"),
    /** Shanghai fork. */
    SHANGHAI(true, "Shanghai"),
    /** Cancun fork. */
    CANCUN(true, "Cancun"),
    /** Prague fork. */
    PRAGUE(true, "Prague"),
    /** Osaka fork. */
    OSAKA(true, "Osaka"),
    /** BPO1 fork. */
    BPO1(true, "BPO1"),
    /** BPO2 fork. */
    BPO2(true, "BPO2"),
    /** BPO3 fork. */
    BPO3(true, "BPO3"),
    /** BPO4 fork. */
    BPO4(true, "BPO4"),
    /** BPO5 fork. */
    BPO5(true, "BPO5"),
    /** BinTrie fork - Binary Trie state tree. */
    BINTRIE(false, "BinTrie"),
    /** Amsterdam fork. */
    AMSTERDAM(false, "Amsterdam"),
    /** Bogota fork. */
    BOGOTA(false, "Bogota"),
    /** Polis fork. (from the greek form of an earlier incarnation of the city of Istanbul. */
    POLIS(false, "Polis"),
    /** Bangkok fork. */
    BANGKOK(false, "Bangkok"),
    /** Development fork, for accepted and unscheduled EIPs. */
    FUTURE_EIPS(false, "FutureEips"),
    /** Developmental fork, for experimental EIPs. */
    EXPERIMENTAL_EIPS(false, "ExperimentalEips");

    final boolean finalized;
    final String description;

    MainnetHardforkId(final boolean finalized, final String description) {
      this.finalized = finalized;
      this.description = description;
    }

    @Override
    public boolean finalized() {
      return finalized;
    }

    @Override
    public String description() {
      return description;
    }

    /**
     * The most recent finalized mainnet hardfork Besu supports. This will change across versions
     * and will be updated after mainnet activations.
     *
     * @return the most recently activated mainnet spec.
     */
    public static MainnetHardforkId mostRecent() {
      return Stream.of(MainnetHardforkId.values())
          .filter(MainnetHardforkId::finalized)
          .max(Comparator.naturalOrder())
          .orElseThrow();
    }
  }
}
