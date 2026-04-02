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
package org.hyperledger.besu.config;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Optional;

/** The enum Network name. */
public enum NetworkDefinition {
  /** Mainnet network name. */
  MAINNET(
      "/mainnet.json",
      1, // chain id
      1, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /** Sepolia network name. */
  SEPOLIA(
      "/sepolia.json",
      11155111, // chain id
      11155111, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /** Hoodi network name. */
  HOODI(
      "/hoodi.json",
      560048, // chain id
      560048, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /**
   * EPHEMERY network name. The actual networkId used is calculated based on this default value and
   * the current time. <a href="https://ephemery.dev/">Ephemery developer info</a>
   */
  EPHEMERY(
      "/ephemery.json",
      39438135, // chain id
      39438135, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /**
   * Linea mainnet network name <a
   * href="https://docs.linea.build/get-started/how-to/run-a-node/besu">Linea Besu developer
   * info</a>
   */
  LINEA_MAINNET(
      "/linea-mainnet.json",
      59144, // chain id
      59144, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /** Linea sepolia network name */
  LINEA_SEPOLIA(
      "/linea-sepolia.json",
      59141, // chain id
      59141, // network id
      true, // can snap sync
      true, // native required
      60_000_000L), // target gas limit
  /** LUKSO mainnet network name. */
  LUKSO(
      "/lukso.json",
      42, // chain id
      42, // network id
      true, // can snap sync
      false, // native required
      60_000_000L), // target gas limit
  /** Dev network name. */
  DEV(
      "/dev.json",
      1337, // chain id
      2018, // network id
      false, // can snap sync
      false, // native required
      60_000_000L), // target gas limit
  /** Future EIPs network name. */
  FUTURE_EIPS(
      "/future.json",
      2022, // chain id
      2022, // network id
      false, // can snap sync
      false, // native required
      60_000_000L), // target gas limit
  /** Experimental EIPs network name. */
  EXPERIMENTAL_EIPS(
      "/experimental.json",
      2023, // chain id
      2023, // network id
      false, // can snap sync
      false, // native required
      60_000_000L); // target gas limit

  private final String genesisFile;
  private final long chainId;
  private final long networkId;
  private final boolean canSnapSync;
  private final boolean nativeRequired;
  private final long targetGasLimit;

  NetworkDefinition(
      final String genesisFile,
      final long chainId,
      final long networkId,
      final boolean canSnapSync,
      final boolean nativeRequired,
      final long targetGasLimit) {
    this.genesisFile = genesisFile;
    this.chainId = chainId;
    this.networkId = networkId;
    this.canSnapSync = canSnapSync;
    this.nativeRequired = nativeRequired;
    this.targetGasLimit = targetGasLimit;
  }

  /**
   * Gets genesis file.
   *
   * @return the genesis file
   */
  public String getGenesisFile() {
    return genesisFile;
  }

  /**
   * Gets chain id.
   *
   * @return the chain id
   */
  public BigInteger getChainId() {
    return BigInteger.valueOf(chainId);
  }

  /**
   * Gets network id.
   *
   * @return the network id
   */
  public BigInteger getNetworkId() {
    return BigInteger.valueOf(networkId);
  }

  /**
   * Can SNAP sync boolean.
   *
   * @return the boolean
   */
  public boolean canSnapSync() {
    return canSnapSync;
  }

  /**
   * Normalize string.
   *
   * @return the string
   */
  public String normalize() {
    String n = name().toLowerCase(Locale.ROOT);
    return n.substring(0, 1).toUpperCase(Locale.ROOT) + n.substring(1);
  }

  /**
   * Determines whether the network requires the use of native libraries or native support.
   *
   * <p>Certain networks may require specific platform-level optimizations or library dependencies,
   * referred to as "native requirements." This method indicates whether such requirements are
   * necessary for the current network.
   *
   * @return {@code true} if the network requires native support or libraries; {@code false}
   *     otherwise.
   */
  public boolean hasNativeRequirements() {
    return nativeRequired;
  }

  /**
   * Gets target gas limit.
   *
   * @return the target gas limit
   */
  public long getTargetGasLimit() {
    return targetGasLimit;
  }

  /**
   * From chain id.
   *
   * @param chainId the chain id
   * @return the optional
   */
  public static Optional<NetworkDefinition> fromChainId(final BigInteger chainId) {
    for (final NetworkDefinition network : NetworkDefinition.values()) {
      if (network.getChainId().equals(chainId)) {
        return Optional.of(network);
      }
    }
    return Optional.empty();
  }
}
