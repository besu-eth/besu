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
package org.hyperledger.besu.cli.config;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 * Enum for profile names which are bundled. Each profile corresponds to a bundled configuration
 * file.
 */
public enum InternalProfileName {
  /** The 'PERFORMANCE' profile */
  PERFORMANCE("profiles/performance.toml"),
  /** The 'PERFORMANCE_RPC' profile */
  PERFORMANCE_RPC("profiles/performance-rpc.toml"),
  /** The 'STAKER' profile */
  STAKER("profiles/staker.toml"),
  /** The 'MINIMALIST_STAKER' profile */
  MINIMALIST_STAKER("profiles/minimalist-staker.toml"),
  /** The 'ENTERPRISE' profile */
  ENTERPRISE("profiles/enterprise-private.toml"),
  /** The 'PRIVATE' profile */
  PRIVATE("profiles/enterprise-private.toml");

  private final String configFile;

  /**
   * Returns the InternalProfileName that matches the given name, ignoring case.
   *
   * @param name The profile name
   * @return Optional InternalProfileName if found, otherwise empty
   */
  public static Optional<InternalProfileName> valueOfIgnoreCase(final String name) {
    return Arrays.stream(values())
        .filter(profile -> profile.name().equalsIgnoreCase(name))
        .findFirst();
  }

  /**
   * Returns the set of internal profile names as lowercase.
   *
   * @return Set of internal profile names
   */
  public static Set<String> getInternalProfileNames() {
    return Arrays.stream(InternalProfileName.values())
        .map(InternalProfileName::name)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
  }

  /**
   * Constructs a new ProfileName.
   *
   * @param configFile the configuration file corresponding to the profile
   */
  InternalProfileName(final String configFile) {
    this.configFile = configFile;
  }

  /**
   * Gets the configuration file corresponding to the profile.
   *
   * @return the configuration file
   */
  public String getConfigFile() {
    return configFile;
  }

  @Override
  public String toString() {
    return StringUtils.capitalize(name().replaceAll("_", " "));
  }
}
