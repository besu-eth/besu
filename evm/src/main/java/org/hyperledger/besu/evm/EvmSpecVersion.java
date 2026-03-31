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
package org.hyperledger.besu.evm;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId;

import java.util.Comparator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The enum Evm spec version. */
public enum EvmSpecVersion {
  /** Frontier evm spec version. */
  FRONTIER(MainnetHardforkId.FRONTIER, Integer.MAX_VALUE, Integer.MAX_VALUE),
  /** Homestead evm spec version. */
  HOMESTEAD(MainnetHardforkId.HOMESTEAD, Integer.MAX_VALUE, Integer.MAX_VALUE),
  /** Tangerine Whistle evm spec version. */
  TANGERINE_WHISTLE(MainnetHardforkId.TANGERINE_WHISTLE, Integer.MAX_VALUE, Integer.MAX_VALUE),
  /** Spurious Dragon evm spec version. */
  SPURIOUS_DRAGON(
      MainnetHardforkId.SPURIOUS_DRAGON, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Byzantium evm spec version. */
  BYZANTIUM(MainnetHardforkId.BYZANTIUM, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Constantinople evm spec version. */
  CONSTANTINOPLE(
      MainnetHardforkId.CONSTANTINOPLE, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Petersburg / ConstantinopleFix evm spec version. */
  PETERSBURG(MainnetHardforkId.PETERSBURG, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Istanbul evm spec version. */
  ISTANBUL(MainnetHardforkId.ISTANBUL, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Berlin evm spec version */
  BERLIN(MainnetHardforkId.BERLIN, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** London evm spec version. */
  LONDON(MainnetHardforkId.LONDON, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Paris evm spec version. */
  PARIS(MainnetHardforkId.PARIS, Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON, Integer.MAX_VALUE),
  /** Shanghai evm spec version. */
  SHANGHAI(
      MainnetHardforkId.SHANGHAI,
      Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON,
      Limits.MAX_INITCODE_SIZE_SHANGHAI),
  /** Cancun evm spec version. */
  CANCUN(
      MainnetHardforkId.CANCUN,
      Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON,
      Limits.MAX_INITCODE_SIZE_SHANGHAI),
  /** Prague evm spec version. */
  PRAGUE(
      MainnetHardforkId.PRAGUE,
      Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON,
      Limits.MAX_INITCODE_SIZE_SHANGHAI),
  /** Osaka evm spec version. */
  OSAKA(
      MainnetHardforkId.OSAKA,
      Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON,
      Limits.MAX_INITCODE_SIZE_SHANGHAI),
  /** BinTrie evm spec version - Binary Trie stateless gas costs (EIP-4762). */
  BINTRIE(
      MainnetHardforkId.BINTRIE,
      Limits.MAX_CODE_SIZE_SPURIOUS_DRAGON,
      Limits.MAX_INITCODE_SIZE_SHANGHAI),
  /** Amsterdam evm spec version. */
  AMSTERDAM(
      MainnetHardforkId.AMSTERDAM,
      Limits.MAX_CODE_SIZE_AMSTERDAM,
      Limits.MAX_INITCODE_SIZE_AMSTERDAM),
  /** Bogota evm spec version. */
  BOGOTA(
      MainnetHardforkId.BOGOTA, Limits.MAX_CODE_SIZE_AMSTERDAM, Limits.MAX_INITCODE_SIZE_AMSTERDAM),
  /** Polis evm spec version. */
  POLIS(
      MainnetHardforkId.POLIS, Limits.MAX_CODE_SIZE_AMSTERDAM, Limits.MAX_INITCODE_SIZE_AMSTERDAM),
  /** Bangkok evm spec version. */
  BANGKOK(
      MainnetHardforkId.BANGKOK,
      Limits.MAX_CODE_SIZE_AMSTERDAM,
      Limits.MAX_INITCODE_SIZE_AMSTERDAM),
  /** Development fork for unscheduled EIPs */
  FUTURE_EIPS(
      MainnetHardforkId.FUTURE_EIPS,
      Limits.MAX_CODE_SIZE_AMSTERDAM,
      Limits.MAX_INITCODE_SIZE_AMSTERDAM),
  /** Development fork for EIPs that are not yet accepted to Mainnet */
  EXPERIMENTAL_EIPS(
      MainnetHardforkId.EXPERIMENTAL_EIPS,
      Limits.MAX_CODE_SIZE_AMSTERDAM,
      Limits.MAX_INITCODE_SIZE_AMSTERDAM);

  /** Constants for contract size limits */
  private interface Limits {
    /** Maximum deployed code size: 24KiB (EIP-170, Spurious Dragon through Osaka). */
    int MAX_CODE_SIZE_SPURIOUS_DRAGON = 0x6000;

    /** Maximum initcode size: 48KiB (EIP-3860, Shanghai through Osaka). */
    int MAX_INITCODE_SIZE_SHANGHAI = 0xC000;

    /** Maximum deployed code size: 32KiB (EIP-7954, Amsterdam onwards). */
    int MAX_CODE_SIZE_AMSTERDAM = 0x8000;

    /** Maximum initcode size: 64KiB (EIP-7954, Amsterdam onwards). */
    int MAX_INITCODE_SIZE_AMSTERDAM = 0x10000;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EvmSpecVersion.class);

  /** What hardfork did this VM version first show up in? */
  final HardforkId initialHardfork;

  /** Maximum size of deployed code */
  final int maxCodeSize;

  /** Maximum size of initcode */
  final int maxInitcodeSize;

  /** The Version warned. */
  boolean versionWarned = false;

  EvmSpecVersion(
      final HardforkId initialHardfork, final int maxCodeSize, final int maxInitcodeSize) {
    this.initialHardfork = initialHardfork;
    this.maxCodeSize = maxCodeSize;
    this.maxInitcodeSize = maxInitcodeSize;
  }

  /**
   * What is the "default" version of EVM that should be made. Newer versions of Besu will adjust
   * this to reflect mainnet fork development.
   *
   * @return the current mainnet for as of the release of this version of Besu
   */
  public static EvmSpecVersion defaultVersion() {
    EvmSpecVersion answer = null;
    for (EvmSpecVersion version : EvmSpecVersion.values()) {
      if (version.initialHardfork.finalized()) {
        answer = version;
      }
    }
    return answer;
  }

  /**
   * Gets max deployed code size this EVM supports.
   *
   * @return the max eof version
   */
  public int getMaxCodeSize() {
    return maxCodeSize;
  }

  /**
   * Gets max initcode size this EVM supports.
   *
   * @return the max eof version
   */
  public int getMaxInitcodeSize() {
    return maxInitcodeSize;
  }

  /**
   * Name of the fork, in execution-spec-tests form
   *
   * @return name of the fork
   */
  public String getName() {
    return initialHardfork.name();
  }

  /**
   * Description of the fork
   *
   * @return description
   */
  public String getDescription() {
    return initialHardfork.description();
  }

  /** Maybe warn version. */
  @SuppressWarnings("AlreadyChecked") // false positive
  public void maybeWarnVersion() {
    if (versionWarned) {
      return;
    }

    if (!initialHardfork.finalized()) {
      LOGGER.error(
          "****** Not for Production Network Use ******\nExecuting code from EVM Spec Version {}, which has not been finalized.\n****** Not for Production Network Use ******",
          this.name());
    }
    versionWarned = true;
  }

  /**
   * Calculate a spec version from a text fork name.
   *
   * @param name The name of the fork, such as "shanghai" or "berlin"
   * @return the EVM spec version for that fork, or null if no fork matched.
   */
  public static EvmSpecVersion fromName(final String name) {
    for (var version : EvmSpecVersion.values()) {
      if (version.name().equalsIgnoreCase(name)) {
        return version;
      }
    }
    return null;
  }

  /**
   * The most recent deployed evm supported by the library. This will change across versions and
   * will be updated after mainnet activations.
   *
   * @return the most recently activated mainnet spec.
   */
  public static EvmSpecVersion mostRecent() {
    return Stream.of(EvmSpecVersion.values())
        .filter(v -> v.initialHardfork.finalized())
        .max(Comparator.naturalOrder())
        .orElseThrow();
  }
}
